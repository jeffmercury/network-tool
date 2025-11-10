package com.example.network_tool.repo;

import com.example.network_tool.db.DuckDb;
import com.example.network_tool.model.Models.*;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class DuckDbRepo {
    private final DuckDb duck;

    public DuckDbRepo(DuckDb duck) {
        this.duck = duck;
    }

    // To DO : need to load crime events based on lpr near by
    // Just loads tables moved out of profile service so its easier for me and
    // others
    // to read, Need to use trim to make sure I have a standard way in seeing the
    // data that may have dashes or spaces
    private static String nvl(String s) {
        return s == null ? "" : s;
    }

    public Person loadPerson(String ssn) throws SQLException {
        String sql = """
                    SELECT TRIM(ssn) AS ssn,
                           UPPER(TRIM(firstname)) AS first,
                           UPPER(TRIM(COALESCE(middlename,''))) AS middle,
                           UPPER(TRIM(lastname)) AS last,
                           UPPER(TRIM(dl)) AS dl,
                           UPPER(TRIM(address_line1)) AS addr1
                    FROM people
                    WHERE TRIM(ssn) = TRIM(?)
                """;
        try (PreparedStatement ps = duck.conn().prepareStatement(sql)) {
            ps.setString(1, ssn);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next())
                    return null;
                Person p = new Person();
                p.ssn = rs.getString("ssn");
                p.first = rs.getString("first");
                p.middle = rs.getString("middle");
                p.last = rs.getString("last");
                p.dl = rs.getString("dl");
                p.addr1 = rs.getString("addr1");
                p.name = (p.first + " " + (p.middle.isBlank() ? "" : (p.middle + " ")) + p.last)
                        .trim().replaceAll(" +", " ");
                return p;
            }
        }
    }

    public List<Phone> loadPhones(String ssn) throws SQLException {
        String sql = """
                    SELECT TRIM(ssn) AS ssn,
                           TRIM(phone) AS msisdn,
                           UPPER(TRIM(phone_type)) AS phone_type,
                           UPPER(TRIM(device_make)) AS device_make,
                           UPPER(TRIM(device_model)) AS device_model,
                           UPPER(TRIM(mac)) AS mac,
                           TRIM(imsi) AS imsi
                    FROM phone_contracts
                    WHERE TRIM(ssn) = TRIM(?)
                """;
        List<Phone> out = new ArrayList<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql)) {
            ps.setString(1, ssn);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Phone p = new Phone();
                    p.ssn = rs.getString("ssn");
                    p.msisdn = nvl(rs.getString("msisdn"));
                    p.type = nvl(rs.getString("phone_type"));
                    p.make = nvl(rs.getString("device_make"));
                    p.model = nvl(rs.getString("device_model"));
                    p.mac = nvl(rs.getString("mac"));
                    p.imsi = nvl(rs.getString("imsi"));
                    out.add(p);
                }
            }
        }
        return out;
    }

    public List<Employer> loadEmployers(String ssn) throws SQLException {
        String sql = """
                    SELECT UPPER(TRIM(employer_name)) AS employer_name,
                           UPPER(TRIM(employer_address)) AS employer_address
                    FROM tax_employers
                    WHERE TRIM(ssn) = TRIM(?)
                """;
        List<Employer> out = new ArrayList<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql)) {
            ps.setString(1, ssn);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Employer e = new Employer();
                    e.name = nvl(rs.getString("employer_name"));
                    e.address = nvl(rs.getString("employer_address"));
                    out.add(e);
                }
            }
        }
        return out;
    }

    public List<Vehicle> loadVehiclesByDL(String dl) throws SQLException {
        if (dl == null || dl.isBlank())
            return List.of();
        String sql = """
                    SELECT UPPER(TRIM(vin_norm)) AS vin,
                           UPPER(TRIM(plate_norm)) AS plate,
                           UPPER(TRIM(make)) AS make,
                           UPPER(TRIM(model)) AS model,
                           CAST(year AS BIGINT) AS year
                    FROM vehicles
                    WHERE UPPER(TRIM(owner_dl)) = ?
                """;
        List<Vehicle> out = new ArrayList<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql)) {
            ps.setString(1, dl);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Vehicle v = new Vehicle();
                    v.vin = nvl(rs.getString("vin"));
                    v.plate = nvl(rs.getString("plate"));
                    v.make = nvl(rs.getString("make"));
                    v.model = nvl(rs.getString("model"));
                    v.year = (Long) rs.getObject("year");
                    out.add(v);
                }
            }
        }
        return out;
    }

    // NEW: create TEMP ankle_poi for this SSN
    public void createTempAnklePoi(String ssn) throws SQLException {
        try (Statement st = duck.conn().createStatement()) {
            st.execute("DROP TABLE IF EXISTS ankle_poi");
        }
        String makeTmp = """
                CREATE TEMP TABLE ankle_poi AS
                SELECT ts, lat, lon
                FROM ankle
                WHERE person_ssn = ?
                  AND lat IS NOT NULL AND lon IS NOT NULL
                """;
        try (PreparedStatement ps = duck.conn().prepareStatement(makeTmp)) {
            ps.setString(1, ssn);
            ps.execute();
        }
    }

    public List<BizVisit> loadBusinessesLinkedForSsn(
            String ssn, double radiusMeters, int minDistinctHours, int limit) throws SQLException {
        String sql = """
                WITH hits AS (
                  SELECT
                    b.biz_id,
                    b.name,
                    b.line1,
                    b.b_lat AS lat,
                    b.b_lon AS lon,
                    a.ts,
                    date_trunc('hour', a.ts) AS hour_bucket
                  FROM ankle a
                  JOIN businesses b
                    -- DuckDB Spatial: distance between two POINT geometries (meters)
                    ON ST_Distance_Sphere(
                         ST_Point(a.lon, a.lat),       -- (x=lon, y=lat)
                         ST_Point(b.b_lon, b.b_lat)
                       ) <= ?
                  WHERE TRIM(a.person_ssn) = TRIM(?)
                ),
                agg AS (
                  SELECT
                    biz_id,
                    ANY_VALUE(name)  AS name,
                    ANY_VALUE(line1) AS line1,
                    ANY_VALUE(lat)   AS lat,
                    ANY_VALUE(lon)   AS lon,
                    MIN(ts)          AS first_ts,
                    MAX(ts)          AS last_ts,
                    COUNT(*)         AS pings,
                    COUNT(DISTINCT hour_bucket) AS hour_buckets
                  FROM hits
                  GROUP BY biz_id
                )
                SELECT
                  biz_id,
                  name,
                  line1,
                  lat,
                  lon,
                  first_ts,
                  last_ts,
                  pings,
                  hour_buckets,
                  'ankle proximity' AS method
                FROM agg
                WHERE hour_buckets >= ?
                ORDER BY hour_buckets DESC, pings DESC
                LIMIT ?
                """;

        try (var ps = duck.conn().prepareStatement(sql)) {
            ps.setDouble(1, radiusMeters);
            ps.setString(2, ssn);
            ps.setInt(3, minDistinctHours);
            ps.setInt(4, limit);

            List<BizVisit> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new BizVisit(
                            rs.getString("biz_id"),
                            rs.getString("name"),
                            rs.getString("line1"),
                            rs.getDouble("lat"),
                            rs.getDouble("lon"),
                            rs.getTimestamp("first_ts").toInstant(),
                            rs.getTimestamp("last_ts").toInstant(),
                            rs.getInt("pings"),
                            rs.getInt("hour_buckets"),
                            rs.getString("method")));
                }
            }
            return out;
        }
    }

    public List<AnklePing> loadAnkle(String ssn) throws SQLException {
        String sql = """
                    SELECT ts, CAST(lat AS DOUBLE) AS lat, CAST(lon AS DOUBLE) AS lon
                    FROM ankle
                    WHERE TRIM(person_ssn) = TRIM(?)
                    ORDER BY ts
                """;
        List<AnklePing> out = new ArrayList<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql)) {
            ps.setString(1, ssn);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Timestamp ts = rs.getTimestamp("ts");
                    out.add(new AnklePing(ts.toInstant(), rs.getDouble("lat"), rs.getDouble("lon")));
                }
            }
        }
        return out;
    }

    public List<Business> loadBusinesses() throws SQLException {
        String sql = """
                    SELECT UPPER(TRIM(biz_id)) AS biz_id,
                           UPPER(TRIM(name)) AS name,
                           UPPER(TRIM(line1)) AS line1,
                           CAST(b_lat AS DOUBLE) AS lat,
                           CAST(b_lon AS DOUBLE) AS lon,
                           UPPER(TRIM(owner_firs)) AS owner_firs,
                           UPPER(TRIM(owner_last)) AS owner_last
                    FROM businesses
                """;
        List<Business> out = new ArrayList<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Business b = new Business();
                b.bizId = nvl(rs.getString("biz_id"));
                b.name = nvl(rs.getString("name"));
                b.line1 = nvl(rs.getString("line1"));
                b.lat = rs.getDouble("lat");
                b.lon = rs.getDouble("lon");
                b.ownerFirst = nvl(rs.getString("owner_firs"));
                b.ownerLast = nvl(rs.getString("owner_last"));
                out.add(b);
            }
        }
        return out;
    }

    public List<OwnedBiz> loadOwnedBusinessesBySsn(String ssn) throws SQLException {
        String sql = """
                    SELECT
                        b.biz_id,
                        b.name,
                        b.line1,
                        CAST(b.b_lat AS DOUBLE) AS lat,
                        CAST(b.b_lon AS DOUBLE) AS lon
                    FROM people p
                    JOIN businesses b
                      ON UPPER(TRIM(b.owner_firs)) = UPPER(TRIM(p.firstname))
                     AND UPPER(TRIM(b.owner_last)) = UPPER(TRIM(p.lastname))
                    WHERE TRIM(p.ssn) = TRIM(?)
                """;

        List<OwnedBiz> out = new ArrayList<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql)) {
            ps.setString(1, ssn);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new OwnedBiz(
                            rs.getString("biz_id"),
                            rs.getString("name"),
                            rs.getString("line1"),
                            rs.getDouble("lat"),
                            rs.getDouble("lon"),
                            "owner_name_fl" // same reason tag you had before
                    ));
                }
            }
        }
        return out;
    }

    public List<WifiEvent> loadWifiEvents() throws SQLException {
        String sql = """
                    SELECT ts, CAST(s_lat AS DOUBLE) AS s_lat, CAST(s_lon AS DOUBLE) AS s_lon, device_mac,
                           ssid_1, ssid_2, ssid_3, ssid_4, ssid_5, ssid_6, ssid_7, ssid_8, ssid_9, ssid_10
                    FROM wifi_events_raw
                """;
        List<WifiEvent> out = new ArrayList<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                WifiEvent e = new WifiEvent();
                e.ts = rs.getTimestamp("ts").toInstant();
                e.lat = rs.getDouble("s_lat");
                e.lon = rs.getDouble("s_lon");
                e.deviceMac = nvl(rs.getString("device_mac"));
                for (int i = 1; i <= 10; i++) {
                    String s = rs.getString("ssid_" + i);
                    if (s != null && !s.isBlank())
                        e.ssids.add(s.trim().toUpperCase());
                }
                out.add(e);
            }
        }
        return out;
    }

    public List<LprHit> loadLpr() throws SQLException {
        String sql = """
                    SELECT ts, CAST(lat AS DOUBLE) AS lat, CAST(lon AS DOUBLE) AS lon,
                           UPPER(TRIM(sensor_id)) AS sensor_id,
                           UPPER(TRIM(direction)) AS direction,
                           UPPER(TRIM(plate_state)) AS plate_state,
                           TRIM(plate_raw) AS plate_raw,
                           UPPER(TRIM(plate_norm)) AS plate_norm
                    FROM lpr
                """;
        List<LprHit> out = new ArrayList<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                LprHit h = new LprHit();
                h.ts = rs.getTimestamp("ts").toInstant();
                h.lat = rs.getDouble("lat");
                h.lon = rs.getDouble("lon");
                h.sensorId = nvl(rs.getString("sensor_id"));
                h.direction = nvl(rs.getString("direction"));
                h.plateState = nvl(rs.getString("plate_state"));
                h.plateRaw = nvl(rs.getString("plate_raw"));
                h.plateNorm = nvl(rs.getString("plate_norm"));
                out.add(h);
            }
        }
        return out;
    }

    public List<Crime> loadCrimes() throws SQLException {
        String sql = """
                    SELECT report_id, c_lat AS lat, c_lon AS lon, pre_text, post_text, file_path
                    FROM crime_reports
                """;
        List<Crime> out = new ArrayList<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Crime c = new Crime();
                c.reportId = nvl(rs.getString("report_id"));
                c.lat = rs.getDouble("lat");
                c.lon = rs.getDouble("lon");
                c.preText = nvl(rs.getString("pre_text"));
                c.postText = nvl(rs.getString("post_text"));
                c.filePath = nvl(rs.getString("file_path"));
                out.add(c);
            }
        }
        return out;
    }

    public Set<String> findPeopleByHomeAddr(String addrUpper, String excludeSsn) throws SQLException {
        String sql = """
                    SELECT TRIM(ssn) AS ssn
                    FROM people
                    WHERE UPPER(TRIM(address_line1)) = ?
                      AND TRIM(ssn) <> ?
                """;
        Set<String> out = new LinkedHashSet<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql)) {
            ps.setString(1, addrUpper);
            ps.setString(2, excludeSsn);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next())
                    out.add(rs.getString("ssn"));
            }
        }
        return out;
    }

    public Set<String> findFilersByAddress(String addrUpper, String excludeSsn) throws SQLException {
        String sql = """
                    SELECT TRIM(ssn) AS ssn
                    FROM tax_employers
                    WHERE UPPER(TRIM(filer_address)) = ?
                      AND TRIM(ssn) <> ?
                """;
        Set<String> out = new LinkedHashSet<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql)) {
            ps.setString(1, addrUpper);
            ps.setString(2, excludeSsn);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next())
                    out.add(rs.getString("ssn"));
            }
        }
        return out;
    }

    public Set<String> findPeopleByEmployerAddress(String addrUpper, String excludeSsn) throws SQLException {
        String sql = """
                    SELECT DISTINCT TRIM(ssn) AS ssn
                    FROM tax_employers
                    WHERE UPPER(TRIM(employer_address)) = ?
                      AND TRIM(ssn) <> ?
                """;
        Set<String> out = new LinkedHashSet<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql)) {
            ps.setString(1, addrUpper);
            ps.setString(2, excludeSsn);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next())
                    out.add(rs.getString("ssn"));
            }
        }
        return out;
    }

    public Set<String> findCoworkersByEmployerNames(Set<String> empNamesUpper, String excludeSsn) throws SQLException {
        if (empNamesUpper.isEmpty())
            return Set.of();
        String in = empNamesUpper.stream().map(s -> "?").collect(Collectors.joining(","));
        String sql = "SELECT TRIM(ssn) AS ssn FROM tax_employers " +
                "WHERE UPPER(TRIM(employer_name)) IN (" + in + ") AND TRIM(ssn) <> ?";
        Set<String> out = new LinkedHashSet<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql)) {
            int i = 1;
            for (String n : empNamesUpper)
                ps.setString(i++, n);
            ps.setString(i, excludeSsn);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next())
                    out.add(rs.getString("ssn"));
            }
        }
        return out;
    }

    public Map<String, PersonMini> loadPersonMini(Set<String> ssns) throws SQLException {
        if (ssns.isEmpty())
            return Map.of();
        String qMarks = ssns.stream().map(s -> "?").collect(Collectors.joining(","));
        String sql = "SELECT TRIM(ssn) AS ssn, " +
                " UPPER(TRIM(firstname)) AS first, UPPER(TRIM(COALESCE(middlename,''))) AS middle, UPPER(TRIM(lastname)) AS last, "
                +
                " UPPER(TRIM(address_line1)) AS addr1, UPPER(TRIM(dl)) AS dl " +
                "FROM people WHERE TRIM(ssn) IN (" + qMarks + ")";
        Map<String, PersonMini> out = new HashMap<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql)) {
            int i = 1;
            for (String s : ssns)
                ps.setString(i++, s);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String s = rs.getString("ssn");
                    String first = nvl(rs.getString("first"));
                    String middle = nvl(rs.getString("middle"));
                    String last = nvl(rs.getString("last"));
                    String name = (first + " " + (middle.isBlank() ? "" : (middle + " ")) + last).trim()
                            .replaceAll(" +", " ");
                    out.put(s,
                            new PersonMini(s, name, nvl(rs.getString("addr1")), first, last, nvl(rs.getString("dl"))));
                }
            }
        }
        // fill from tax filers if missing
        Set<String> missing = new HashSet<>(ssns);
        missing.removeAll(out.keySet());
        if (!missing.isEmpty()) {
            String q = missing.stream().map(x -> "?").collect(Collectors.joining(","));
            String sql2 = "SELECT TRIM(ssn) AS ssn, " +
                    " UPPER(TRIM(filer_first)) AS first, UPPER(TRIM(COALESCE(filer_middle,''))) AS middle, UPPER(TRIM(filer_last)) AS last, "
                    +
                    " UPPER(TRIM(filer_address)) AS addr1 " +
                    "FROM tax_employers WHERE TRIM(ssn) IN (" + q + ")";
            try (PreparedStatement ps = duck.conn().prepareStatement(sql2)) {
                int i = 1;
                for (String s : missing)
                    ps.setString(i++, s);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String s = rs.getString("ssn");
                        String first = nvl(rs.getString("first"));
                        String middle = nvl(rs.getString("middle"));
                        String last = nvl(rs.getString("last"));
                        String name = (first + " " + (middle.isBlank() ? "" : (middle + " ")) + last).trim()
                                .replaceAll(" +", " ");
                        out.put(s, new PersonMini(s, name, nvl(rs.getString("addr1")), first, last, null));
                    }
                }
            }
        }
        return out;
    }

    public Map<String, List<Phone>> loadPhonesFor(Set<String> ssns) throws SQLException {
        if (ssns.isEmpty())
            return Map.of();
        String q = ssns.stream().map(s -> "?").collect(Collectors.joining(","));
        String sql = "SELECT TRIM(ssn) AS ssn, TRIM(phone) AS msisdn, " +
                " UPPER(TRIM(phone_type)) AS phone_type, UPPER(TRIM(device_make)) AS device_make, " +
                " UPPER(TRIM(device_model)) AS device_model, UPPER(TRIM(mac)) AS mac, TRIM(imsi) AS imsi " +
                "FROM phone_contracts WHERE TRIM(ssn) IN (" + q + ")";
        Map<String, List<Phone>> out = new HashMap<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql)) {
            int i = 1;
            for (String s : ssns)
                ps.setString(i++, s);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Phone p = new Phone();
                    p.ssn = rs.getString("ssn");
                    p.msisdn = nvl(rs.getString("msisdn"));
                    p.type = nvl(rs.getString("phone_type"));
                    p.make = nvl(rs.getString("device_make"));
                    p.model = nvl(rs.getString("device_model"));
                    p.mac = nvl(rs.getString("mac"));
                    p.imsi = nvl(rs.getString("imsi"));
                    out.computeIfAbsent(p.ssn, k -> new ArrayList<>()).add(p);
                }
            }
        }
        return out;
    }

    public Map<String, List<Vehicle>> loadVehiclesFor(Map<String, String> ssnToDl) throws SQLException {
        if (ssnToDl.isEmpty())
            return Map.of();
        String sql = """
                    SELECT UPPER(TRIM(owner_dl)) AS owner_dl,
                           UPPER(TRIM(vin_norm))  AS vin,
                           UPPER(TRIM(plate_norm))AS plate,
                           UPPER(TRIM(make))      AS make,
                           UPPER(TRIM(model))     AS model,
                           CAST(year AS BIGINT)   AS year
                    FROM vehicles
                    WHERE UPPER(TRIM(owner_dl)) = ?
                """;
        Map<String, List<Vehicle>> out = new HashMap<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql)) {
            for (Map.Entry<String, String> e : ssnToDl.entrySet()) {
                ps.setString(1, e.getValue());
                try (ResultSet rs = ps.executeQuery()) {
                    List<Vehicle> list = new ArrayList<>();
                    while (rs.next()) {
                        Vehicle v = new Vehicle();
                        v.vin = nvl(rs.getString("vin"));
                        v.plate = nvl(rs.getString("plate"));
                        v.make = nvl(rs.getString("make"));
                        v.model = nvl(rs.getString("model"));
                        v.year = (Long) rs.getObject("year");
                        list.add(v);
                    }
                    out.put(e.getKey(), list);
                }
            }
        }
        return out;
    }

    // Linked conversations for this SSN
    public List<ConvoLink> loadConversations(String ssn) throws SQLException {
        String sql = """
                    WITH poi AS (
                      SELECT UPPER(TRIM(imsi)) AS imsi,
                             REGEXP_REPLACE(TRIM(phone), '[^0-9]', '', 'g') AS msisdn_norm
                      FROM phone_contracts
                      WHERE TRIM(ssn) = TRIM(?)
                    ),
                    mine AS (
                      SELECT t.ts, t.type, t.duration_sec, t.imsi, t.imsi_from, t.imsi_to
                      FROM telco t
                      WHERE t.type IN ('SMS','CALL')
                        AND (
                          t.imsi IN (SELECT imsi FROM poi)
                          OR t.msisdn_norm IN (SELECT msisdn_norm FROM poi)
                        )
                    ),
                    edges AS (
                      SELECT
                        CASE
                          WHEN m.imsi = m.imsi_from THEN m.imsi_to
                          WHEN m.imsi = m.imsi_to   THEN m.imsi_from
                          ELSE NULL
                        END AS other_imsi,
                        m.imsi AS self_imsi,
                        m.type,
                        m.duration_sec,
                        m.ts,
                        CASE
                          WHEN m.imsi = m.imsi_from THEN 'OUT'
                          WHEN m.imsi = m.imsi_to   THEN 'IN'
                          ELSE NULL
                        END AS dir
                      FROM mine m
                    )
                    SELECT
                      e.other_imsi                                        AS other_imsi,
                      COUNT(*)                                            AS events,
                      SUM(CASE WHEN e.type='CALL' THEN 1 ELSE 0 END)      AS calls,
                      SUM(CASE WHEN e.type='SMS'  THEN 1 ELSE 0 END)      AS sms,
                      SUM(CASE WHEN e.type='CALL' THEN COALESCE(e.duration_sec,0) ELSE 0 END) AS duration_sec,
                      SUM(CASE WHEN e.dir='OUT' THEN 1 ELSE 0 END)        AS out_events,
                      SUM(CASE WHEN e.dir='IN'  THEN 1 ELSE 0 END)        AS in_events,
                      MIN(e.ts)                                           AS first_ts,
                      MAX(e.ts)                                           AS last_ts,
                      pc_other.ssn                                        AS other_ssn,
                      pc_other.phone                                      AS other_msisdn_raw,
                      pc_other.phone_norm                                 AS other_msisdn_norm,
                      UPPER(TRIM(COALESCE(ppl.firstname,''))) || ' ' || UPPER(TRIM(COALESCE(ppl.lastname,''))) AS other_name
                    FROM edges e
                    LEFT JOIN phone_contracts pc_other ON pc_other.imsi = e.other_imsi
                    LEFT JOIN people ppl ON TRIM(ppl.ssn) = TRIM(pc_other.ssn)
                    WHERE e.other_imsi IS NOT NULL
                    GROUP BY e.other_imsi, pc_other.ssn, pc_other.phone, pc_other.phone_norm, other_name
                    ORDER BY events DESC, last_ts DESC
                    LIMIT 100
                """;

        List<ConvoLink> out = new ArrayList<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql)) {
            ps.setString(1, ssn);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ConvoLink c = new ConvoLink();
                    c.otherImsi = nvl(rs.getString("other_imsi"));
                    c.otherSsn = nvl(rs.getString("other_ssn"));
                    c.otherName = nvl(rs.getString("other_name")).trim().replaceAll(" +", " ");
                    c.otherMsisdnRaw = nvl(rs.getString("other_msisdn_raw"));
                    c.otherMsisdnNorm = nvl(rs.getString("other_msisdn_norm"));
                    c.events = rs.getLong("events");
                    c.calls = rs.getLong("calls");
                    c.sms = rs.getLong("sms");
                    c.durationSec = rs.getLong("duration_sec");
                    c.outEvents = rs.getLong("out_events");
                    c.inEvents = rs.getLong("in_events");
                    Timestamp f = rs.getTimestamp("first_ts");
                    Timestamp l = rs.getTimestamp("last_ts");
                    c.firstTs = f == null ? null : f.toInstant();
                    c.lastTs = l == null ? null : l.toInstant();
                    out.add(c);
                }
            }
        }
        return out;
    }

    public Map<String, Employer> loadPrimaryEmployer(Set<String> ssns) throws SQLException {
        if (ssns.isEmpty())
            return Map.of();
        String q = ssns.stream().map(s -> "?").collect(Collectors.joining(","));
        String sql = "SELECT TRIM(ssn) AS ssn, UPPER(TRIM(employer_name)) AS n, UPPER(TRIM(employer_address)) AS a " +
                "FROM tax_employers WHERE TRIM(ssn) IN (" + q
                + ") AND employer_address IS NOT NULL AND TRIM(employer_address) <> ''";
        Map<String, Map<String, Long>> countByPair = new HashMap<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql)) {
            int i = 1;
            for (String s : ssns)
                ps.setString(i++, s);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String s = rs.getString("ssn");
                    String key = nvl(rs.getString("n")) + "|" + nvl(rs.getString("a"));
                    countByPair.computeIfAbsent(s, k -> new HashMap<>()).merge(key, 1L, Long::sum);
                }
            }
        }
        Map<String, Employer> out = new HashMap<>();
        for (Map.Entry<String, Map<String, Long>> e : countByPair.entrySet()) {
            String bestKey = e.getValue().entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed()
                            .thenComparing(Map.Entry.comparingByKey()))
                    .map(Map.Entry::getKey)
                    .findFirst().orElse(null);
            if (bestKey != null) {
                String[] parts = bestKey.split("\\|", -1);
                Employer emp = new Employer();
                emp.name = parts.length > 0 ? parts[0] : "";
                emp.address = parts.length > 1 ? parts[1] : "";
                out.put(e.getKey(), emp);
            }
        }
        return out;
    }

    public List<WifiSpot> loadWifiNearbyForSsn(
            String ssn, double radiusMeters, int minuteWindow, int limit) throws SQLException {

        String sql = """
                  WITH poi_macs AS (
                    SELECT UPPER(TRIM(mac)) AS mac
                    FROM phone_contracts
                    WHERE TRIM(ssn)=TRIM(?)
                      AND mac IS NOT NULL AND TRIM(mac) <> ''
                  ),
                  ank AS (
                    SELECT ts, CAST(lat AS DOUBLE) AS lat, CAST(lon AS DOUBLE) AS lon
                    FROM ankle
                    WHERE TRIM(person_ssn)=TRIM(?)
                      AND lat IS NOT NULL AND lon IS NOT NULL
                  ),
                  bounds AS (
                    SELECT MIN(ts) AS min_ts, MAX(ts) AS max_ts FROM ank
                  ),
                  cand AS (
                    SELECT
                      w.ts,
                      CAST(w.s_lat AS DOUBLE) AS lat,
                      CAST(w.s_lon AS DOUBLE) AS lon,
                      UPPER(TRIM(w.device_mac)) AS device_mac,
                      COALESCE(NULLIF(TRIM(w.ssid_1),''), NULLIF(TRIM(w.ssid_2),'')) AS ssid
                    FROM ank a
                    JOIN wifi_events_raw w
                      -- global time prune (zone-map friendly)
                      ON w.ts BETWEEN (SELECT min_ts FROM bounds) AND (SELECT max_ts FROM bounds)
                     AND w.device_mac IN (SELECT mac FROM poi_macs)
                     -- local time window per ankle (parameter-friendly)
                     AND ABS(date_diff('minute', a.ts, w.ts)) <= ?
                     -- cheap bounding box first
                     AND w.s_lat BETWEEN a.lat - (?/111000.0) AND a.lat + (?/111000.0)
                     AND w.s_lon BETWEEN a.lon - ((?/111000.0)/GREATEST(COS(radians(a.lat)), 1e-6))
                                     AND a.lon + ((?/111000.0)/GREATEST(COS(radians(a.lat)), 1e-6))
                     -- exact distance last
                     AND ST_Distance_Sphere(ST_Point(a.lon,a.lat), ST_Point(w.s_lon,w.s_lat)) <= ?
                  )
                  SELECT
                    device_mac,
                    MIN(ts)  AS first_ts,
                    MAX(ts)  AS last_ts,
                    COUNT(*) AS hits,
                    ANY_VALUE(ssid) AS ssid,
                    AVG(lat) AS lat,
                    AVG(lon) AS lon
                  FROM cand
                  GROUP BY device_mac
                  ORDER BY hits DESC, last_ts DESC
                  LIMIT ?
                """;

        try (var ps = duck.conn().prepareStatement(sql)) {
            int i = 1;
            ps.setString(i++, ssn); // poi_macs
            ps.setString(i++, ssn); // ank
            ps.setInt(i++, minuteWindow); // local time window
            ps.setDouble(i++, radiusMeters); // lat_box (lower)
            ps.setDouble(i++, radiusMeters); // lat_box (upper)
            ps.setDouble(i++, radiusMeters); // lon_box (lower)
            ps.setDouble(i++, radiusMeters); // lon_box (upper)
            ps.setDouble(i++, radiusMeters); // exact distance
            ps.setInt(i++, limit);

            List<WifiSpot> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    WifiSpot s = new WifiSpot();
                    s.firstTs = rs.getTimestamp("first_ts").toInstant();
                    s.lastTs = rs.getTimestamp("last_ts").toInstant();
                    s.hits = rs.getLong("hits");
                    s.lat = rs.getDouble("lat");
                    s.lon = rs.getDouble("lon");
                    String one = rs.getString("ssid");
                    if (one != null && !one.isBlank())
                        s.ssids.add(one);
                    s.via = "wifi proximity";
                    out.add(s);
                }
            }
            return out;
        }
    }

    public List<LprView> loadLprSightingsForSsn(

            String ssn, double radiusMeters, int minuteWindow, int limit) throws SQLException {

        String sql = """
                  WITH ank AS (
                    SELECT ts, CAST(lat AS DOUBLE) AS lat, CAST(lon AS DOUBLE) AS lon
                    FROM ankle
                    WHERE TRIM(person_ssn)=TRIM(?)
                      AND lat IS NOT NULL AND lon IS NOT NULL
                  ),
                  bounds AS (
                    SELECT MIN(ts) AS min_ts, MAX(ts) AS max_ts FROM ank
                  ),
                  pairs AS (
                    SELECT
                      l.ts  AS lpr_ts,
                      a.ts  AS ankle_ts,
                      ST_Distance_Sphere(ST_Point(a.lon,a.lat), ST_Point(l.lon,l.lat)) AS dist_m,
                      CAST(l.lat AS DOUBLE)  AS lat,
                      CAST(l.lon AS DOUBLE)  AS lon,
                      UPPER(TRIM(l.sensor_id))  AS sensor_id,
                      UPPER(TRIM(l.direction))  AS direction,
                      UPPER(TRIM(l.plate_state)) AS plate_state,
                      TRIM(l.plate_raw)         AS plate_raw
                    FROM ank a
                    JOIN lpr l
                      -- global time prune
                      ON l.ts BETWEEN (SELECT min_ts FROM bounds) AND (SELECT max_ts FROM bounds)
                     -- local time window (parameter-friendly)
                     AND ABS(date_diff('minute', a.ts, l.ts)) <= ?
                     -- cheap bounding box
                     AND l.lat BETWEEN a.lat - (?/111000.0) AND a.lat + (?/111000.0)
                     AND l.lon BETWEEN a.lon - ((?/111000.0)/GREATEST(COS(radians(a.lat)), 1e-6))
                                  AND a.lon + ((?/111000.0)/GREATEST(COS(radians(a.lat)), 1e-6))
                     -- exact distance
                     AND ST_Distance_Sphere(ST_Point(a.lon,a.lat), ST_Point(l.lon,l.lat)) <= ?
                  ),
                  ranked AS (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY lpr_ts ORDER BY dist_m) AS rn
                    FROM pairs
                  )
                  SELECT
                    lpr_ts,
                    ankle_ts,
                    dist_m,
                    lat,
                    lon,
                    sensor_id,
                    direction,
                    plate_state,
                    plate_raw
                  FROM ranked
                  WHERE rn = 1
                  ORDER BY lpr_ts DESC
                  LIMIT ?
                """;

        try (var ps = duck.conn().prepareStatement(sql)) {
            int i = 1;
            ps.setString(i++, ssn); // ank
            ps.setInt(i++, minuteWindow); // local window
            ps.setDouble(i++, radiusMeters); // lat_box (lower)
            ps.setDouble(i++, radiusMeters); // lon_box (lower)
            ps.setDouble(i++, radiusMeters); // lon_box (upper)
            ps.setDouble(i++, radiusMeters); // exact distance
            ps.setInt(i++, limit);

            List<LprView> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    LprView v = new LprView();
                    v.ts = rs.getTimestamp("lpr_ts").toInstant();
                    v.ankleTs = rs.getTimestamp("ankle_ts").toInstant();
                    v.distM = rs.getDouble("dist_m");
                    v.lat = rs.getDouble("lat");
                    v.lon = rs.getDouble("lon");
                    v.sensorId = nvl(rs.getString("sensor_id"));
                    v.direction = nvl(rs.getString("direction"));
                    v.plateState = nvl(rs.getString("plate_state"));
                    v.plateRaw = nvl(rs.getString("plate_raw"));
                    v.method = "ankle+LPR proximity";
                    v.confirmed = true;
                    out.add(v);
                }
            }
            return out;
        }

    }

    public List<Business> loadBusinessesInBox(double minLat, double maxLat, double minLon, double maxLon)
            throws SQLException {
        String sql = """
                    SELECT biz_id, name, line1,
                           CAST(b_lat AS DOUBLE) AS lat,
                           CAST(b_lon AS DOUBLE) AS lon,
                           owner_firs, owner_last
                    FROM businesses
                    WHERE b_lat IS NOT NULL
                      AND b_lon IS NOT NULL
                      AND b_lat BETWEEN ? AND ?
                      AND b_lon BETWEEN ? AND ?
                """;

        List<Business> out = new ArrayList<>();
        try (PreparedStatement ps = duck.conn().prepareStatement(sql)) {
            ps.setDouble(1, minLat);
            ps.setDouble(2, maxLat);
            ps.setDouble(3, minLon);
            ps.setDouble(4, maxLon);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Business b = new Business();
                    b.bizId = rs.getString("biz_id");
                    b.name = rs.getString("name");
                    b.line1 = rs.getString("line1");
                    b.lat = rs.getDouble("lat");
                    b.lon = rs.getDouble("lon");
                    b.ownerFirst = rs.getString("owner_firs");
                    b.ownerLast = rs.getString("owner_last");
                    out.add(b);
                }
            }
        }
        return out;
    }

    public List<WifiEvent> loadWifiEventsByBounds(
            Instant minTs,
            Instant maxTs,
            double minLat, double maxLat,
            double minLon, double maxLon,
            Set<String> deviceMacs, // may be empty or null = no MAC filter
            int limit // pass Integer.MAX_VALUE for "no limit"
    ) throws SQLException {

        boolean useMacs = deviceMacs != null && !deviceMacs.isEmpty();

        String macClause = useMacs
                ? " AND device_mac IN (" +
                        deviceMacs.stream().map(x -> "?").collect(java.util.stream.Collectors.joining(",")) +
                        ")"
                : "";

        String limClause = (limit > 0 && limit < Integer.MAX_VALUE) ? " LIMIT ?" : "";

        String sql = """
                SELECT
                    ts,
                    CAST(s_lat AS DOUBLE) AS s_lat,
                    CAST(s_lon AS DOUBLE) AS s_lon,
                    device_mac,
                    ssid_1, ssid_2, ssid_3, ssid_4, ssid_5,
                    ssid_6, ssid_7, ssid_8, ssid_9, ssid_10
                FROM wifi_events_raw
                WHERE ts BETWEEN ? AND ?
                  AND s_lat BETWEEN ? AND ?
                  AND s_lon BETWEEN ? AND ?
                """ + macClause + limClause;

        try (PreparedStatement ps = duck.conn().prepareStatement(sql)) {
            int i = 1;
            ps.setTimestamp(i++, java.sql.Timestamp.from(minTs));
            ps.setTimestamp(i++, java.sql.Timestamp.from(maxTs));
            ps.setDouble(i++, minLat);
            ps.setDouble(i++, maxLat);
            ps.setDouble(i++, minLon);
            ps.setDouble(i++, maxLon);

            if (useMacs) {
                for (String mac : deviceMacs)
                    ps.setString(i++, mac); // already normalized upstream
            }
            if (!limClause.isBlank()) {
                ps.setInt(i++, limit);
            }

            List<WifiEvent> out = new java.util.ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    WifiEvent e = new WifiEvent();
                    e.ts = rs.getTimestamp("ts").toInstant();
                    e.lat = rs.getDouble("s_lat");
                    e.lon = rs.getDouble("s_lon");
                    e.deviceMac = nvl(rs.getString("device_mac")); // keep as-is; no UPPER/TRIM

                    // collect non-empty SSIDs as-is (no upper/trim since normalized)
                    for (int k = 1; k <= 10; k++) {
                        String s = rs.getString("ssid_" + k);
                        if (s != null && !s.isBlank())
                            e.ssids.add(s);
                    }
                    out.add(e);
                }
            }
            return out;
        }

    }

    // DuckDbRepo.java
    public List<LprHit> loadLprByBounds(
            Instant minTs,
            Instant maxTs,
            double minLat, double maxLat,
            double minLon, double maxLon,
            Set<String> plateNorms, // optional; pass empty set to ignore
            int limit // safety cap
    ) throws SQLException {

        String base = """
                SELECT
                  ts,
                  CAST(lat AS DOUBLE)       AS lat,
                  CAST(lon AS DOUBLE)       AS lon,
                  sensor_id,
                  direction,
                  plate_state,
                  plate_raw,
                  plate_norm
                FROM lpr
                WHERE ts BETWEEN ? AND ?
                  AND lat BETWEEN ? AND ?
                  AND lon BETWEEN ? AND ?
                """;

        // Add optional plate filter
        StringBuilder sql = new StringBuilder(base);
        if (plateNorms != null && !plateNorms.isEmpty()) {
            String in = plateNorms.stream().map(s -> "?").collect(java.util.stream.Collectors.joining(","));
            sql.append(" AND plate_norm IN (").append(in).append(") ");
        }
        sql.append(" ORDER BY ts DESC ");
        if (limit > 0)
            sql.append(" LIMIT ? ");

        try (PreparedStatement ps = duck.conn().prepareStatement(sql.toString())) {
            int i = 1;
            ps.setTimestamp(i++, java.sql.Timestamp.from(minTs));
            ps.setTimestamp(i++, java.sql.Timestamp.from(maxTs));
            ps.setDouble(i++, minLat);
            ps.setDouble(i++, maxLat);
            ps.setDouble(i++, minLon);
            ps.setDouble(i++, maxLon);

            if (plateNorms != null && !plateNorms.isEmpty()) {
                for (String p : plateNorms)
                    ps.setString(i++, p);
            }
            if (limit > 0)
                ps.setInt(i++, limit);

            List<LprHit> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    LprHit h = new LprHit();
                    h.ts = rs.getTimestamp("ts").toInstant();
                    h.lat = rs.getDouble("lat");
                    h.lon = rs.getDouble("lon");
                    h.sensorId = rs.getString("sensor_id");
                    h.direction = rs.getString("direction");
                    h.plateState = rs.getString("plate_state");
                    h.plateRaw = rs.getString("plate_raw");
                    h.plateNorm = rs.getString("plate_norm");
                    out.add(h);
                }
            }
            return out;
        }
    }

}
