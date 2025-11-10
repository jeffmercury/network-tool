package com.example.network_tool.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class Models {
    private Models() {
    }

    // Put all the models in same file, to create separate ones would be overkill
    // for an exercise
    public static final class Person {
        public String ssn;
        public String first;
        public String middle;
        public String last;
        public String name;
        public String dl;
        public String addr1;
    }

    public static final class Phone {
        public String ssn;
        public String msisdn;
        public String type;
        public String make;
        public String model;
        public String mac;
        public String imsi;
    }

    public static final class Vehicle {
        public String vin;
        public String plate;
        public String make;
        public String model;
        public Long year;
    }

    public static final class Employer {
        public String name;
        public String address;
    }

    public static final class Business {
        public String bizId;
        public String name;
        public String line1;
        public String ownerFirst;
        public String ownerLast;
        public double lat;
        public double lon;
    }

    public record AnklePing(Instant ts, double lat, double lon) {
    }

    public static final class WifiEvent {
        public Instant ts;
        public double lat;
        public double lon;
        public String deviceMac;
        public List<String> ssids = new ArrayList<>();
    }

    public static final class WifiSpot {
        public double lat;
        public double lon;
        public List<String> ssids = new ArrayList<>();
        public Instant firstTs;
        public Instant lastTs;
        public long hits;
        public String via;
    }

    public static final class LprHit {
        public Instant ts;
        public double lat;
        public double lon;
        public String sensorId;
        public String direction;
        public String plateState;
        public String plateRaw;
        public String plateNorm;
    }

    public static final class LprView {
        public Instant ts;
        public Instant ankleTs;
        public Double distM;
        public double lat;
        public double lon;
        public String sensorId;
        public String direction;
        public String plateState;
        public String plateRaw;
        public String method;
        public boolean confirmed;
    }

    public static final class Crime {
        public String reportId;
        public String preText;
        public String postText;
        public String filePath;
        public double lat;
        public double lon;
    }

    public static final class CrimeMatch {
        public String reportId;
        public String preText;
        public String postText;
        public String filePath;
        public String via;
        public Double distM;
        public Double lat;
        public Double lon;
        public Instant ankleTs;
        public Instant crimeTs;
    }

    public static final class OwnedBiz {
        public String bizId;
        public String name;
        public String line1;
        public String via;
        public double lat;
        public double lon;

        public OwnedBiz() {
        }

        public OwnedBiz(String bizId, String name, String line1, double lat, double lon, String via) {
            this.bizId = bizId;
            this.name = name;
            this.line1 = line1;
            this.lat = lat;
            this.lon = lon;
            this.via = via;
        }
    }

    public static final class BizVisit {
        public String bizId;
        public String name;
        public String line1;
        public String via;
        public double lat;
        public double lon;
        public Instant firstTs;
        public Instant lastTs;
        public int pings;
        public int visitHours;

        public BizVisit() {
        }

        public BizVisit(String bizId, String name, String line1, double lat, double lon,
                Instant firstTs, Instant lastTs, int pings, int visitHours, String via) {
            this.bizId = bizId;
            this.name = name;
            this.line1 = line1;
            this.lat = lat;
            this.lon = lon;
            this.firstTs = firstTs;
            this.lastTs = lastTs;
            this.pings = pings;
            this.visitHours = visitHours;
            this.via = via;
        }
    }

    public record PersonMini(String ssn, String name, String addr1, String first, String last, String dl) {
    }

    public static final class Connected {
        public String ssn;
        public String name;
        public String addressLine1;
        public String employerName;
        public String employerAddress;
        public List<String> vias = new ArrayList<>();
        public List<Phone> phones = new ArrayList<>();
        public List<Vehicle> vehicles = new ArrayList<>();
        public List<OwnedBiz> businesses = new ArrayList<>();
    }

    public static final class PeopleConnections {
        public List<Connected> cards = new ArrayList<>();

        public PeopleConnections() {
        }

        public PeopleConnections(List<Connected> cards) {
            this.cards = cards;
        }
    }

    // Using theses to set up min radius before returning result to get more
    // accurate data
    // Otherwise we will pick up everthing, give us a good aprx
    public static final class Consts {
        public static final double BIZ_RADIUS_M = 80.0;
        public static final int MIN_VISIT_HOURS = 2;
        public static final double WIFI_RADIUS_M = 120.0;
        public static final int WIFI_MINUTES = 10;
        public static final double LPR_RADIUS_M = 120.0;
        public static final int LPR_MINUTES = 10;
        public static final double CRIME_RADIUS_M = 150.0;

        private Consts() {
        }
    }

    public static final class ConvoLink {
        public String otherImsi;
        public String otherMsisdnRaw;
        public String otherMsisdnNorm;
        public String otherSsn;
        public String otherName;
        public long events;
        public long calls;
        public long sms;
        public long durationSec;
        public long outEvents;
        public long inEvents;
        public Instant firstTs;
        public Instant lastTs;
        public String via = "telco";
    }

    public record Bounds(double minLat, double maxLat, double minLon, double maxLon) {
    }
}
