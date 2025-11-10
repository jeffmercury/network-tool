package com.example.network_tool.mapper;

import com.example.network_tool.model.Models.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ProfileJsonMapper {
    private final ObjectMapper om = new ObjectMapper();

    private static String nvl(String s) { return s == null ? "" : s; }
    private static String nullIfBlank(String s) { return (s == null || s.isBlank()) ? null : s; }

    private void putStr(ObjectNode n, String field, String value) {
        if (value == null) n.putNull(field); else n.put(field, value);
    }
    private void putNum(ObjectNode n, String field, Long value) {
        if (value == null) n.putNull(field); else n.put(field, value);
    }
    private void putNum(ObjectNode n, String field, Double value) {
        if (value == null) n.putNull(field); else n.put(field, value);
    }

    public ObjectNode personNode(Person p) {
        ObjectNode node = om.createObjectNode();
        putStr(node, "ssn", p.ssn);
        putStr(node, "name", p.name);
        putStr(node, "dl", p.dl);
        ObjectNode addr = om.createObjectNode();
        putStr(addr, "line1", nvl(p.addr1));
        addr.putNull("city"); addr.putNull("state"); addr.putNull("zip");
        node.set("address", addr);
        return node;
    }

    public ArrayNode listPhones(List<Phone> phones, boolean includeVia) {
        ArrayNode arr = om.createArrayNode();
        for (Phone p : phones) {
            ObjectNode n = om.createObjectNode();
            putStr(n, "msisdn", nullIfBlank(p.msisdn));
            putStr(n, "imsi",   nullIfBlank(p.imsi));
            putStr(n, "mac",    nullIfBlank(p.mac));
            putStr(n, "type",   nullIfBlank(p.type));
            putStr(n, "make",   nullIfBlank(p.make));
            putStr(n, "model",  nullIfBlank(p.model));
            if (includeVia) putStr(n, "via", "contract");
            arr.add(n);
        }
        return arr;
    }

    public ArrayNode listVehicles(List<Vehicle> vehicles, String via) {
        ArrayNode arr = om.createArrayNode();
        for (Vehicle v : vehicles) {
            ObjectNode n = om.createObjectNode();
            putStr(n, "vin",   nullIfBlank(v.vin));
            putStr(n, "plate", nullIfBlank(v.plate));
            putStr(n, "make",  nullIfBlank(v.make));
            putStr(n, "model", nullIfBlank(v.model));
            putNum(n, "year",  v.year);
            if (via != null) putStr(n, "via", via);
            arr.add(n);
        }
        return arr;
    }

    public ArrayNode listEmployers(List<Employer> emps, String via) {
        ArrayNode arr = om.createArrayNode();
        for (Employer e : emps) {
            ObjectNode n = om.createObjectNode();
            putStr(n, "name",    nullIfBlank(e.name));
            putStr(n, "address", nullIfBlank(e.address));
            putStr(n, "via", via);
            arr.add(n);
        }
        return arr;
    }

    public ArrayNode listOwnedBiz(List<OwnedBiz> owned) {
        ArrayNode arr = om.createArrayNode();
        for (OwnedBiz b : owned) {
            ObjectNode n = om.createObjectNode();
            putStr(n, "biz_id", b.bizId);
            putStr(n, "name",   b.name);
            putNum(n, "lat",    b.lat);
            putNum(n, "lon",    b.lon);
            putStr(n, "line1",  b.line1);
            putStr(n, "via",    b.via);
            arr.add(n);
        }
        return arr;
    }

    public ArrayNode listBizVisits(List<BizVisit> visits) {
        ArrayNode arr = om.createArrayNode();
        for (BizVisit v : visits) {
            ObjectNode n = om.createObjectNode();
            putStr(n, "biz_id", v.bizId);
            putStr(n, "name",   v.name);
            putStr(n, "line1",  v.line1);
            putNum(n, "lat",    v.lat);
            putNum(n, "lon",    v.lon);
            putStr(n, "first_seen", v.firstTs == null ? null : v.firstTs.toString());
            putStr(n, "last_seen",  v.lastTs  == null ? null : v.lastTs.toString());
            putNum(n, "pings",      (long) v.pings);
            putNum(n, "visit_hours",(long) v.visitHours);
            putStr(n, "via", v.via);
            arr.add(n);
        }
        return arr;
    }

    public ArrayNode listWifi(List<WifiSpot> spots) {
        ArrayNode arr = om.createArrayNode();
        for (WifiSpot w : spots) {
            ObjectNode n = om.createObjectNode();
            putNum(n, "lat", w.lat);
            putNum(n, "lon", w.lon);
            ArrayNode ss = om.createArrayNode();
            w.ssids.forEach(ss::add);
            n.set("ssids", ss);
            putStr(n, "first_ts", w.firstTs == null ? null : w.firstTs.toString());
            putStr(n, "last_ts",  w.lastTs  == null ? null : w.lastTs.toString());
            n.put("hits", w.hits);
            putStr(n, "via",  w.via);
            arr.add(n);
        }
        return arr;
    }

    public ArrayNode listLpr(List<LprView> lpr) {
        ArrayNode arr = om.createArrayNode();
        for (LprView v : lpr) {
            ObjectNode n = om.createObjectNode();
            putStr(n, "ts", v.ts.toString());
            putNum(n, "lat", v.lat);
            putNum(n, "lon", v.lon);
            putStr(n, "method", v.method);
            n.put("confirmed_by_ankle", v.confirmed);
            putStr(n, "ankle_ts", v.ankleTs == null ? null : v.ankleTs.toString());
            if (v.distM == null) n.putNull("dist_m"); else n.put("dist_m", v.distM);
            putStr(n, "sensor_id", v.sensorId);
            putStr(n, "direction", v.direction);
            putStr(n, "plate_state", v.plateState);
            putStr(n, "plate_raw", v.plateRaw);
            arr.add(n);
        }
        return arr;
    }

    public ArrayNode listCrimes(List<CrimeMatch> crimes) {
        ArrayNode arr = om.createArrayNode();
        for (CrimeMatch c : crimes) {
            ObjectNode n = om.createObjectNode();
            putStr(n, "report_id", c.reportId);
            n.putNull("report_ts");
            putNum(n, "lat", c.lat);
            putNum(n, "lon", c.lon);
            putStr(n, "ankle_ts", c.ankleTs == null ? null : c.ankleTs.toString());
            putNum(n, "dist_m", c.distM);
            putStr(n, "pre_text",  c.preText);
            putStr(n, "post_text", c.postText);
            putStr(n, "file_path", c.filePath);
            putStr(n, "via", c.via);
            arr.add(n);
        }
        return arr;
    }

    public ArrayNode listConnected(PeopleConnections pc) {
        ArrayNode arr = om.createArrayNode();
        for (Connected c : pc.cards) {
            ObjectNode n = om.createObjectNode();
            putStr(n, "ssn",  c.ssn);
            putStr(n, "name", c.name);
            ArrayNode via = om.createArrayNode();
            c.vias.forEach(via::add);
            n.set("via", via);
            putStr(n, "address", c.addressLine1);
            n.set("phones",   listPhones(c.phones, false));
            n.set("vehicles", listVehicles(c.vehicles, null));
            ArrayNode bz = om.createArrayNode();
            for (OwnedBiz b : c.businesses) {
                ObjectNode bn = om.createObjectNode();
                putStr(bn, "biz_id", b.bizId);
                putStr(bn, "name",   b.name);
                putStr(bn, "line1",  b.line1);
                putNum(bn, "lat",    b.lat);
                putNum(bn, "lon",    b.lon);
                putStr(bn, "via",    b.via);
                bz.add(bn);
            }
            n.set("businesses", bz);
            putStr(n, "employer_name",    c.employerName);
            putStr(n, "employer_address", c.employerAddress);
            arr.add(n);
        }
        return arr;
    }


    public ArrayNode listConversations(List<ConvoLink> convos) {
        ArrayNode arr = om.createArrayNode();
        for (ConvoLink c : convos) {
            ObjectNode n = om.createObjectNode();
            putStr(n, "other_imsi",        nullIfBlank(c.otherImsi));
            putStr(n, "other_ssn",         nullIfBlank(c.otherSsn));
            putStr(n, "other_name",        nullIfBlank(c.otherName));
            putStr(n, "other_msisdn",      nullIfBlank(c.otherMsisdnRaw));
            putStr(n, "other_msisdn_norm", nullIfBlank(c.otherMsisdnNorm));
            n.put("events",       c.events);
            n.put("calls",        c.calls);
            n.put("sms",          c.sms);
            n.put("duration_sec", c.durationSec);
            n.put("out_events",   c.outEvents);
            n.put("in_events",    c.inEvents);
            putStr(n, "first_ts", c.firstTs == null ? null : c.firstTs.toString());
            putStr(n, "last_ts",  c.lastTs  == null ? null : c.lastTs.toString());
            putStr(n, "via", c.via);
            arr.add(n);
        }
        return arr;
    }

    public String buildProfileJson(Person poi,
                                   List<Phone> poiPhones,
                                   List<Vehicle> poiVehicles,
                                   List<Employer> poiEmployers,
                                   List<OwnedBiz> owned,
                                   List<BizVisit> bizVisits,
                                   List<WifiSpot> wifiNearby,
                                   List<LprView> lprViews,
                                   List<CrimeMatch> crimeMatches,
                                   PeopleConnections pc
                                  ) throws Exception {
        ObjectNode root = om.createObjectNode();
        root.set("person", personNode(poi));
        root.set("phones", listPhones(poiPhones, true));
        root.set("vehicles", listVehicles(poiVehicles, "DL"));
        root.set("employers", listEmployers(poiEmployers, "tax_filing"));
        root.set("businesses_owned", listOwnedBiz(owned));
        root.set("businesses_linked", listBizVisits(bizVisits));
        root.set("wifi_nearby", listWifi(wifiNearby));
        root.set("lpr_sightings", listLpr(lprViews));
        root.set("crime_matches", listCrimes(crimeMatches));
        root.set("people_connected", listConnected(pc));
        return om.writerWithDefaultPrettyPrinter().writeValueAsString(root);
    }



}
