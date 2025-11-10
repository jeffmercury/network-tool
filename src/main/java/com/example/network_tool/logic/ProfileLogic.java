package com.example.network_tool.logic;

import com.example.network_tool.model.Models.*;
import com.example.network_tool.repo.DuckDbRepo;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Component
public class ProfileLogic {

    // Put all the complex logic here, well call them from profile service, maybe
    // should name it better
    // TO DO 9/6: all of this data is going into memory after we collect from db
    // since we are using streams, we can also use parallel streams to process the
    // data faster with multiple
    // threads, this is good to because there is no I/O since we already have all
    // the data from db
    // let me do that later and test to see improvements

    // Update 9/7 , switched to parallel processing, processing time went from 19
    // second to 5 seconds, so change was affective

    // businesses the person owns
    public List<OwnedBiz> ownedBusinesses(String first, String last, List<Business> all) {
        if (first == null || last == null)
            return List.of();
        String f = first.trim().toUpperCase();
        String l = last.trim().toUpperCase();

        return all.parallelStream()
                .filter(b -> f.equals(b.ownerFirst) && l.equals(b.ownerLast))
                .map(b -> new OwnedBiz(b.bizId, b.name, b.line1, b.lat, b.lon, "owner_name_fl"))
                .collect(Collectors.toList());
    }

    // businesses linked by ANKLE proximity
    public List<BizVisit> businessesLinked(List<AnklePing> ankle, List<Business> all) {
        final double radiusM = Consts.BIZ_RADIUS_M;

        // Build events in parallel
        List<VisitEvent> events = ankle.parallelStream()
                .flatMap(a -> {
                    final Instant hour = a.ts().truncatedTo(ChronoUnit.HOURS);
                    return all.stream()
                            .filter(b -> haversineM(a.lat(), a.lon(), b.lat, b.lon) <= radiusM)
                            .map(b -> new VisitEvent(b, a.ts(), hour));
                })
                .collect(Collectors.toList());

        Map<String, VisitAgg> agg = events.stream()
                .collect(Collectors.groupingBy(
                        ev -> ev.b.bizId,
                        collectingToVisitAgg()));

        Comparator<VisitAgg> byHoursDescThenPingsDesc = Comparator.comparingInt((VisitAgg v) -> v.hourBuckets.size())
                .thenComparingInt(v -> v.pings)
                .reversed();

        List<VisitAgg> filteredSorted = agg.values().stream()
                .filter(a -> a.hourBuckets.size() >= Consts.MIN_VISIT_HOURS)
                .sorted(byHoursDescThenPingsDesc)
                .collect(Collectors.toList());

        return filteredSorted.stream()
                .limit(10)
                .map(v -> new BizVisit(v.b.bizId, v.b.name, v.b.line1, v.b.lat, v.b.lon,
                        v.firstTs, v.lastTs, v.pings, v.hourBuckets.size(), "ankle proximity"))
                .collect(Collectors.toList());
    }

    // WIFI nearby
    public List<WifiSpot> wifiNearby(List<AnklePing> ankle, List<Phone> phones, List<WifiEvent> wifi) {
        final long wMin = Consts.WIFI_MINUTES;
        final double radiusM = Consts.WIFI_RADIUS_M;

        // Normalize phone MACs
        Set<String> phoneMacs = phones.stream()
                .map(p -> normMac(p.mac))
                .filter(s -> !s.isBlank())
                .collect(Collectors.toSet());

        Map<String, WifiAgg> agg = new HashMap<>();

        // Ankle-proximity derived spots
        List<WifiEvt> ankleHits = ankle.parallelStream()
                .flatMap(a -> wifi.stream()
                        .filter(w -> Math.abs(Duration.between(a.ts(), w.ts).toMinutes()) <= wMin)
                        .filter(w -> haversineM(a.lat(), a.lon(), w.lat, w.lon) <= radiusM)
                        .map(w -> new WifiEvt(round(w.lat, 6), round(w.lon, 6), w.ts, w.ssids, "PRS proximity")))
                .collect(Collectors.toList());

        for (WifiEvt e : ankleHits) {
            WifiAgg wa = agg.computeIfAbsent(e.key(), k -> new WifiAgg(e.lat, e.lon));
            wa.hits++;
            wa.vias.add(e.viaTag);
            if (wa.firstTs == null || e.ts.isBefore(wa.firstTs))
                wa.firstTs = e.ts;
            if (wa.lastTs == null || e.ts.isAfter(wa.lastTs))
                wa.lastTs = e.ts;
            wa.ssids.addAll(e.ssids);
        }

        // Device MAC derived spots
        if (!phoneMacs.isEmpty()) {
            List<WifiEvt> macHits = wifi.parallelStream()
                    .filter(w -> !normMac(w.deviceMac).isBlank() && phoneMacs.contains(normMac(w.deviceMac)))
                    .map(w -> new WifiEvt(round(w.lat, 6), round(w.lon, 6), w.ts, w.ssids, "device_mac"))
                    .collect(Collectors.toList());

            for (WifiEvt e : macHits) {
                WifiAgg wa = agg.computeIfAbsent(e.key(), k -> new WifiAgg(e.lat, e.lon));
                wa.hits++;
                wa.vias.add(e.viaTag);
                if (wa.firstTs == null || e.ts.isBefore(wa.firstTs))
                    wa.firstTs = e.ts;
                if (wa.lastTs == null || e.ts.isAfter(wa.lastTs))
                    wa.lastTs = e.ts;
                wa.ssids.addAll(e.ssids);
            }
        }

        return agg.values().stream()
                .sorted(Comparator.comparingLong((WifiAgg wa) -> wa.hits).reversed())
                .limit(200)
                .map(wa -> {
                    WifiSpot s = new WifiSpot();
                    s.lat = wa.lat;
                    s.lon = wa.lon;
                    s.ssids = new ArrayList<>(wa.ssids.stream()
                            .map(String::trim).map(String::toUpperCase)
                            .distinct().sorted().toList());
                    s.firstTs = wa.firstTs;
                    s.lastTs = wa.lastTs;
                    s.hits = wa.hits;
                    s.via = String.join("+", wa.vias);
                    return s;
                })
                .collect(Collectors.toList());
    }

    // LPR sightings
    public List<LprView> lprSightings(List<AnklePing> ankle, List<Vehicle> myVehicles, List<LprHit> allLpr) {
        Set<String> plates = myVehicles.stream()
                .map(v -> v.plate == null ? "" : v.plate.trim().toUpperCase())
                .filter(s -> !s.isBlank())
                .collect(Collectors.toSet());

        List<LprView> out = new ArrayList<>();

        List<LprHit> plateHits = List.of();
        if (!plates.isEmpty()) {
            plateHits = allLpr.stream()
                    .filter(h -> h.plateNorm != null && !h.plateNorm.isBlank())
                    .filter(h -> plates.contains(h.plateNorm.trim().toUpperCase()))
                    .collect(Collectors.toList());

            if (!plateHits.isEmpty()) {
                out = plateHits.parallelStream().map(h -> {
                    AnklePing best = bestAnkleWithinWindow(h.ts, h.lat, h.lon, ankle, Consts.LPR_MINUTES);
                    boolean confirm = false;
                    Double distM = null;
                    Instant ankleTs = null;
                    if (best != null) {
                        double d = haversineM(h.lat, h.lon, best.lat(), best.lon());
                        if (d <= Consts.LPR_RADIUS_M) {
                            confirm = true;
                            distM = d;
                            ankleTs = best.ts();
                        }
                    }
                    LprView v = new LprView();
                    v.ts = h.ts;
                    v.lat = h.lat;
                    v.lon = h.lon;
                    v.sensorId = h.sensorId;
                    v.direction = h.direction;
                    v.plateState = h.plateState;
                    v.plateRaw = h.plateRaw;
                    v.ankleTs = ankleTs;
                    v.distM = distM;
                    v.confirmed = confirm;
                    v.method = "PLATE";
                    return v;
                }).collect(Collectors.toList());
            }
        }

        if (out.isEmpty()) {
            out = allLpr.parallelStream()
                    .map(h -> {
                        AnklePing best = bestAnkleWithinWindow(h.ts, h.lat, h.lon, ankle, Consts.LPR_MINUTES);
                        if (best == null)
                            return null;
                        double d = haversineM(h.lat, h.lon, best.lat(), best.lon());
                        if (d > Consts.LPR_RADIUS_M)
                            return null;

                        LprView v = new LprView();
                        v.ts = h.ts;
                        v.lat = h.lat;
                        v.lon = h.lon;
                        v.sensorId = h.sensorId;
                        v.direction = h.direction;
                        v.plateState = h.plateState;
                        v.plateRaw = h.plateRaw;
                        v.ankleTs = best.ts();
                        v.distM = d;
                        v.confirmed = true;
                        v.method = "ANKLE_PROX";
                        return v;
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }

        out.sort(Comparator.comparing(v -> v.ts));
        return out.stream().limit(500).collect(Collectors.toList());
    }

    // crime matches
    public List<CrimeMatch> matchCrimesSpatial(List<AnklePing> ankle, List<Crime> crimes) {
        List<CrimeMatch> out = crimes.parallelStream()
                .map(c -> {
                    AnklePing best = null;
                    double bestD = Double.POSITIVE_INFINITY;
                    for (AnklePing a : ankle) {
                        double d = haversineM(a.lat(), a.lon(), c.lat, c.lon);
                        if (d < bestD) {
                            bestD = d;
                            best = a;
                        }
                    }
                    if (best != null && bestD <= Consts.CRIME_RADIUS_M) {
                        CrimeMatch m = new CrimeMatch();
                        m.reportId = c.reportId;
                        m.crimeTs = null;
                        m.lat = c.lat;
                        m.lon = c.lon;
                        m.ankleTs = best.ts();
                        m.distM = bestD;
                        m.preText = c.preText;
                        m.postText = c.postText;
                        m.filePath = c.filePath;
                        m.via = "ankle proximity";
                        return m;
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        return out.stream().sorted(Comparator.comparingDouble(cm -> cm.distM)).limit(50).toList();
    }

    public List<CrimeMatch> matchCrimesByLpr(List<LprView> lprViews, List<Crime> crimes) {
        List<CrimeMatch> out = crimes.parallelStream()
                .map(c -> {
                    LprView best = null;
                    double bestD = Double.POSITIVE_INFINITY;
                    for (LprView v : lprViews) {
                        double d = haversineM(v.lat, v.lon, c.lat, c.lon);
                        if (d < bestD) {
                            bestD = d;
                            best = v;
                        }
                    }
                    if (best != null && bestD <= Consts.CRIME_RADIUS_M) {
                        CrimeMatch m = new CrimeMatch();
                        m.reportId = c.reportId;
                        m.crimeTs = null;
                        m.lat = c.lat;
                        m.lon = c.lon;
                        m.ankleTs = best.ankleTs;
                        m.distM = bestD;
                        m.preText = c.preText;
                        m.postText = c.postText;
                        m.filePath = c.filePath;
                        m.via = "lpr proximity";
                        return m;
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        return out.stream().sorted(Comparator.comparingDouble(cm -> cm.distM)).limit(50).toList();
    }

    public List<CrimeMatch> mergeCrimeMatches(List<CrimeMatch> a, List<CrimeMatch> b) {
        Map<String, CrimeMatch> byId = new LinkedHashMap<>();
        for (CrimeMatch m : a) {
            if (m == null)
                continue;
            byId.put(m.reportId, m);
        }
        for (CrimeMatch m : b) {
            if (m == null)
                continue;
            CrimeMatch ex = byId.get(m.reportId);
            if (ex == null) {
                byId.put(m.reportId, m);
            } else {
                if (ex.distM == null || (m.distM != null && m.distM < ex.distM)) {
                    byId.put(m.reportId, m);
                } else if (Objects.equals(ex.distM, m.distM)) {
                    if (ex.via != null && m.via != null && !ex.via.contains(m.via)) {
                        ex.via = ex.via + "+" + m.via;
                    }
                }
            }
        }
        return byId.values().stream()
                .sorted(Comparator.comparingDouble(cm -> cm.distM == null ? Double.POSITIVE_INFINITY : cm.distM))
                .limit(50)
                .collect(Collectors.toList());
    }

    public PeopleConnections findRelatedPeople(DuckDbRepo repo, Person me, List<Employer> myEmployers,
            List<Business> allBusinesses) throws Exception {
        Set<String> related = new LinkedHashSet<>();
        Map<String, Set<String>> vias = new HashMap<>();
        String anchorAddr = nvl(me.addr1).trim().toUpperCase();

        if (!anchorAddr.isBlank()) {
            for (String s : repo.findPeopleByHomeAddr(anchorAddr, me.ssn))
                addRel(related, vias, s, "household");
            for (String s : repo.findFilersByAddress(anchorAddr, me.ssn))
                addRel(related, vias, s, "household");
            for (String s : repo.findPeopleByEmployerAddress(anchorAddr, me.ssn))
                addRel(related, vias, s, "employer_addr_matches_POI");
        }

        Set<String> myEmpNames = myEmployers.stream()
                .map(e -> e.name == null ? "" : e.name.trim().toUpperCase())
                .filter(s -> !s.isBlank()).collect(Collectors.toSet());
        for (String s : repo.findCoworkersByEmployerNames(myEmpNames, me.ssn))
            addRel(related, vias, s, "coworker:tax_employer");

        List<Connected> cards = new ArrayList<>();
        if (!related.isEmpty()) {
            Map<String, PersonMini> basics = repo.loadPersonMini(related);
            Map<String, String> ssnToDl = basics.values().stream()
                    .filter(pm -> pm.dl() != null && !pm.dl().isBlank())
                    .collect(Collectors.toMap(PersonMini::ssn, PersonMini::dl));
            Map<String, List<Phone>> phones = repo.loadPhonesFor(related);
            Map<String, List<Vehicle>> vehicles = repo.loadVehiclesFor(ssnToDl);
            Map<String, Employer> primaryEmployer = repo.loadPrimaryEmployer(related);

            Map<String, List<OwnedBiz>> owned = new HashMap<>();
            related.parallelStream().forEach(s -> {
                PersonMini pm = basics.get(s);
                if (pm != null) {
                    List<OwnedBiz> obs = ownedBusinesses(pm.first(), pm.last(), allBusinesses);
                    synchronized (owned) {
                        owned.put(s, obs);
                    }
                }
            });

            for (String other : related) {
                PersonMini pm = basics.getOrDefault(other, new PersonMini(other, null, null, null, null, null));
                Connected c = new Connected();
                c.ssn = other;
                c.name = pm.name();
                c.addressLine1 = nvl(pm.addr1());
                c.phones = phones.getOrDefault(other, List.of());
                c.vehicles = vehicles.getOrDefault(other, List.of());
                c.businesses = owned.getOrDefault(other, List.of());
                Employer pe = primaryEmployer.get(other);
                c.employerName = pe == null ? null : pe.name;
                c.employerAddress = pe == null ? null : pe.address;
                c.vias = new ArrayList<>(vias.getOrDefault(other, Set.of()));
                cards.add(c);
            }
        }
        return new PeopleConnections(cards);
    }

    // helpers

    private static final class VisitEvent {
        final Business b;
        final Instant ts;
        final Instant hour;

        VisitEvent(Business b, Instant ts, Instant hour) {
            this.b = b;
            this.ts = ts;
            this.hour = hour;
        }
    }

    private static Collector<VisitEvent, ?, VisitAgg> collectingToVisitAgg() {
        return Collectors.collectingAndThen(Collectors.toList(), lst -> {
            VisitAgg va = new VisitAgg(lst.get(0).b);
            va.pings = lst.size();
            for (VisitEvent ev : lst) {
                va.hourBuckets.add(ev.hour);
                if (va.firstTs == null || ev.ts.isBefore(va.firstTs))
                    va.firstTs = ev.ts;
                if (va.lastTs == null || ev.ts.isAfter(va.lastTs))
                    va.lastTs = ev.ts;
            }
            return va;
        });
    }

    private static final class VisitAgg {
        final Business b;
        int pings = 0;
        Instant firstTs = null;
        Instant lastTs = null;
        Set<Instant> hourBuckets = new HashSet<>();

        VisitAgg(Business b) {
            this.b = b;
        }
    }

    private static final class WifiEvt {
        final double lat, lon;
        final Instant ts;
        final Collection<String> ssids;
        final String viaTag;

        WifiEvt(double lat, double lon, Instant ts, Collection<String> ssids, String viaTag) {
            this.lat = lat;
            this.lon = lon;
            this.ts = ts;
            this.ssids = ssids == null ? List.of() : ssids;
            this.viaTag = viaTag;
        }

        String key() {
            return lat + "|" + lon;
        }
    }

    private static final class WifiAgg {
        double lat, lon;
        long hits = 0;
        Instant firstTs = null, lastTs = null;
        Set<String> ssids = new HashSet<>();
        // track whether spot came from ankle, mac, or both
        Set<String> vias = new LinkedHashSet<>();

        WifiAgg(double lat, double lon) {
            this.lat = lat;
            this.lon = lon;
        }
    }

    private static String nvl(String s) {
        return s == null ? "" : s;
    }

    private static void addRel(Set<String> related, Map<String, Set<String>> vias, String ssn, String tag) {
        if (ssn == null || ssn.isBlank())
            return;
        related.add(ssn);
        vias.computeIfAbsent(ssn, k -> new LinkedHashSet<>()).add(tag);
    }

    private static double round(double v, int digits) {
        double p = Math.pow(10, digits);
        return Math.round(v * p) / p;
    }

    private static String normMac(String mac) {
        if (mac == null)
            return "";
        return mac.toUpperCase().replaceAll("[^A-F0-9]", "");
    }

    // We use this to determine distance between two points on earth
    private static double haversineM(double lat1, double lon1, double lat2, double lon2) {
        double R = 6371000.0;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                        * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }

    private static AnklePing closestAnkle(Instant ts, List<AnklePing> ankle) {
        AnklePing best = null;
        long bestAbs = Long.MAX_VALUE;
        for (AnklePing a : ankle) {
            long diff = Math.abs(Duration.between(ts, a.ts()).toMillis());
            if (diff < bestAbs) {
                bestAbs = diff;
                best = a;
            }
        }
        return best;
    }

    // Prefer an ankle ping that is inside the time window and nearest in space.
    private static AnklePing bestAnkleWithinWindow(Instant ts, double lat, double lon,
            List<AnklePing> ankle, long windowMin) {
        AnklePing best = null;
        double bestDist = Double.POSITIVE_INFINITY;
        for (AnklePing a : ankle) {
            long dtMin = Math.abs(Duration.between(ts, a.ts()).toMinutes());
            if (dtMin <= windowMin) {
                double d = haversineM(lat, lon, a.lat(), a.lon());
                if (d < bestDist) {
                    bestDist = d;
                    best = a;
                }
            }
        }
        return best; // may be null
    }

    public List<CrimeMatch> matchCrimesByWifiPhones(List<Phone> phones, List<WifiEvent> wifi, List<Crime> crimes) {
        Set<String> macs = phones.stream()
                .map(p -> normMac(p.mac))
                .filter(s -> !s.isBlank())
                .collect(Collectors.toSet());
        if (macs.isEmpty())
            return List.of();

        List<WifiEvent> myHits = wifi.stream()
                .filter(w -> macs.contains(normMac(w.deviceMac)))
                .collect(Collectors.toList());
        if (myHits.isEmpty())
            return List.of();

        List<CrimeMatch> out = crimes.parallelStream()
                .map(c -> {
                    WifiEvent best = null;
                    double bestD = Double.POSITIVE_INFINITY;
                    for (WifiEvent w : myHits) {
                        double d = haversineM(w.lat, w.lon, c.lat, c.lon);
                        if (d < bestD) {
                            bestD = d;
                            best = w;
                        }
                    }
                    if (best != null && bestD <= Consts.CRIME_RADIUS_M) {
                        CrimeMatch m = new CrimeMatch();
                        m.reportId = c.reportId;
                        m.crimeTs = null;
                        m.lat = c.lat;
                        m.lon = c.lon;
                        m.ankleTs = null;
                        m.distM = bestD;
                        m.preText = c.preText;
                        m.postText = c.postText;
                        m.filePath = c.filePath;
                        m.via = "wifi proximity";
                        return m;
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .sorted(Comparator.comparingDouble(cm -> cm.distM))
                .limit(50)
                .collect(Collectors.toList());

        return out;
    }

    public Bounds computeBoundsWithPadding(List<AnklePing> ankle, double padMeters) {
        if (ankle == null || ankle.isEmpty())
            return null;

        double minLat = Double.POSITIVE_INFINITY, maxLat = Double.NEGATIVE_INFINITY;
        double minLon = Double.POSITIVE_INFINITY, maxLon = Double.NEGATIVE_INFINITY;

        for (AnklePing a : ankle) {
            if (a == null)
                continue;
            if (a.lat() < minLat)
                minLat = a.lat();
            if (a.lat() > maxLat)
                maxLat = a.lat();
            if (a.lon() < minLon)
                minLon = a.lon();
            if (a.lon() > maxLon)
                maxLon = a.lon();
        }

        if (!Double.isFinite(minLat))
            return null;

        // meters → degrees
        double dLat = padMeters / 111_000.0;
        // use the box’s center latitude for lon scaling
        double centerLat = (minLat + maxLat) / 2.0;
        double cos = Math.cos(Math.toRadians(centerLat));
        double dLon = (padMeters / 111_000.0) / Math.max(cos, 1e-6);

        return new Bounds(minLat - dLat, maxLat + dLat, minLon - dLon, maxLon + dLon);
    }

}
