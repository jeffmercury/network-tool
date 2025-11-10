package com.example.network_tool.service;

import com.example.network_tool.logic.ProfileLogic;
import com.example.network_tool.mapper.ProfileJsonMapper;
import com.example.network_tool.model.Models.*;
import com.example.network_tool.repo.DuckDbRepo;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Service
public class ProfileService {
        private final DuckDbRepo repo;
        private final ProfileLogic logic;
        private final ProfileJsonMapper mapper;

        // safety padding for the bounding box
        private static final double BOUNDARY_PADDING_M = 200.0;

        public ProfileService(DuckDbRepo repo, ProfileLogic logic, ProfileJsonMapper mapper) {
                this.repo = repo;
                this.logic = logic;
                this.mapper = mapper;
        }

        public String profileJson(String ssn) {
                try {
                        Person poi = repo.loadPerson(ssn);
                        if (poi == null)
                                return "{ \"error\": \"no result\" }";

                        List<Phone> poiPhones = repo.loadPhones(ssn);
                        List<Employer> poiEmployers = repo.loadEmployers(ssn);
                        List<Vehicle> poiVehicles = repo.loadVehiclesByDL(poi.dl);
                        List<AnklePing> ankle = repo.loadAnkle(ssn);
                        List<Crime> crimes = repo.loadCrimes();

                        // --- Compute ankle time span ---
                        Instant minTs = ankle.stream().map(AnklePing::ts)
                                        .min(Comparator.naturalOrder()).orElse(null);
                        Instant maxTs = ankle.stream().map(AnklePing::ts)
                                        .max(Comparator.naturalOrder()).orElse(null);

                        // --- Compute ankle location bounding box ---
                        Bounds box = logic.computeBoundsWithPadding(ankle, BOUNDARY_PADDING_M);

                        // Load all businesses
                        List<Business> allBusinesses = repo.loadBusinesses();

                        // Load ONLY businesses inside the ankle bounding box
                        List<Business> businessesInBox = (box == null)
                                        ? new ArrayList<>()
                                        : repo.loadBusinessesInBox(box.minLat(), box.maxLat(), box.minLon(),
                                                        box.maxLon());

                        /*
                         * 
                         * List<WifiEvent> wifiEvents = repo.loadWifiEvents();
                         * List<LprHit> allLpr = repo.loadLpr();
                         */

                        List<WifiEvent> wifi = repo.loadWifiEventsByBounds(
                                        minTs, maxTs, box.minLat(), box.maxLat(), box.minLon(), box.maxLon(),
                                        java.util.Set.of(), Integer.MAX_VALUE);

                        List<LprHit> allLpr = repo.loadLprByBounds(
                                        minTs, maxTs,
                                        box.minLat(), box.maxLat(), box.minLon(), box.maxLon(),
                                        java.util.Set.of(),
                                        0);

                        List<OwnedBiz> owned = repo.loadOwnedBusinessesBySsn(ssn);
                        List<BizVisit> bizVisits = logic.businessesLinked(ankle, businessesInBox);

                        List<WifiSpot> wifiNearby = logic.wifiNearby(ankle, poiPhones, wifi);
                        List<LprView> lprViews = logic.lprSightings(ankle, poiVehicles, allLpr);

                        List<CrimeMatch> crimeAnkle = logic.matchCrimesSpatial(ankle, crimes);
                        List<CrimeMatch> crimeLpr = logic.matchCrimesByLpr(lprViews, crimes);
                        List<CrimeMatch> crimeWifi = logic.matchCrimesByWifiPhones(poiPhones, wifi, crimes);
                        List<CrimeMatch> crimeMatches = logic.mergeCrimeMatches(
                                        logic.mergeCrimeMatches(crimeAnkle, crimeLpr), crimeWifi);

                        PeopleConnections pc = logic.findRelatedPeople(repo, poi, poiEmployers, allBusinesses);
                        return mapper.buildProfileJson(poi, poiPhones, poiVehicles, poiEmployers,
                                        owned, bizVisits, wifiNearby, lprViews, crimeMatches, pc);

                } catch (Exception e) {
                        throw new RuntimeException("Profile query failed", e);
                }
        }

}
