package com.cloudcomputing.samza.pitt_cabs;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider
 * to driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask, WindowableTask {

    /* Define per task state here. (kv stores etc) */
    /* private KeyValueStore<Integer, HashSet<Integer>> driverLoc; */
    private KeyValueStore<Integer, Map<String, Object>> drivers;
    /* HashMap<Integer, Map<String, Object>> drivers; */
    private static final double MAX_DIST = 500 * Math.sqrt(2);

    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) throws Exception {
        /* driverLoc = (KeyValueStore<Integer, HashSet<Integer>>) context.getStore("driver-loc"); */
        drivers = (KeyValueStore<Integer, Map<String, Object>>) context.getStore("driver-list");
        /* drivers = new HashMap<>(); */
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        String incomingStream = envelope.getSystemStreamPartition().getStream();
        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
            processDriverLoc((Map<String, Object>) envelope.getMessage());
        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
            processEvent((Map<String, Object>) envelope.getMessage(), collector);
        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }


    private void updateDriverInfo(Map<String, Object> message) {
        int driverId = (int) message.get("driverId");
        if (message.get("type").equals("LEAVING_BLOCK")) drivers.delete(driverId);
        if (message.get("gender") != null
                && message.get("rating") != null
                && message.get("salary") != null
                && drivers.get(driverId) == null) {
            drivers.put(driverId, new HashMap<String, Object>(11, 1.0f));
        }
        if (drivers.get(driverId) != null) {
            Iterator<Map.Entry<String, Object>> it = message.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Object> e = it.next();
                drivers.get(driverId).put(e.getKey(), e.getValue());
            }
            if (message.get("type").equals("RIDE_COMPLETE")) {
                drivers.get(driverId).put("status", "AVAILABLE");
            }
        }
    }

    private void processDriverLoc(Map<String, Object> message) {
        System.out.println("Update from DRIVER_LOC_STREAM");
        updateDriverInfo(message);
    }

    private void processEvent(Map<String, Object> message, MessageCollector collector) {
        String type = (String) message.get("type");
        if (type.equals("RIDE_REQUEST")) {
            match(message, collector);
        } else {
            updateDriverInfo(message);
        }
    }

    private void match(Map<String, Object> message, MessageCollector collector) {
        System.out.println("Match Request");
        int clientId = (int) message.get("clientId");
        int blockId = (int) message.get("blockId");
        int latitude = (int) message.get("latitude");
        int longitude = (int) message.get("longitude");
        String gender_preference = (String) message.get("gender_preference");
        KeyValueIterator<Integer, Map<String, Object>> it = drivers.all();
        /* Iterator<Map.Entry<Integer, Map<String, Object>>> it = drivers.entrySet().iterator(); */
        int bestDriver = -1;
        double bestScore = -1;
        while (it.hasNext()) {
            Entry<Integer, Map<String, Object>> entry = it.next();
            int driverId = entry.getKey();
            Map<String, Object> values = entry.getValue();
            System.out.println("int the match: " + values.entrySet().toString());
            if (values.get("status") != null
                    && values.get("status").equals("AVAILABLE")
                    && values.get("gender") != null
                    && values.get("rating") != null
                    && values.get("salary") != null
                    && (int) values.get("blockId") == blockId) {
                Double distanceScore = 1 - distance(latitude, longitude, (int) values.get("latitude"), (int) values.get("longitude")) / MAX_DIST;
                Double ratingScore = (double) values.get("rating") / 5.0;
                Double salaryScore = 1 - (int) values.get("salary") / 100.0;
                Double genderScore = (gender_preference.equals("N") || gender_preference.equals(values.get("gender"))) ? 1.0 : 0.0;
                Double score = distanceScore * 0.4 + genderScore * 0.2 + ratingScore * 0.2 + salaryScore * 0.2;
                System.out.println("DriverId: " + driverId + " ------DistanceScore: " + distanceScore + " + ------Score: " + score);
                if (score > bestScore) {
                    bestScore = score;
                    bestDriver = driverId;
                }
            }
        }
        it.close();
        System.out.println("---------Ride Request:-------:" + message.entrySet().toString());
        System.out.println("bestMatch: " + bestDriver + " bestScore: " + bestScore);
        if (bestDriver >= 0 && bestScore >= 0) {
            Map<String, String> output = new HashMap<String, String>();
            output.put("clientId", String.valueOf(clientId));
            output.put("driverId", String.valueOf(bestDriver));
            collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, output));
        } else {
            System.out.println("NO Available Driver!!!!!!!");
        }
    }

    private double distance(int x1, int y1, int x2, int y2) {
        return Math.sqrt((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2));
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {
    }
}
