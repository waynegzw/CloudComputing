import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

import java.io.IOException;
import java.util.Map;

public class Coordinator extends Verticle {

    // This integer variable tells you what region you are in
    // 1 for US-E, 2 for US-W, 3 for Singapore
    private static int region = KeyValueLib.region;

    // Default mode: Strongly consistent
    // Options: strong, eventual
    private static String consistencyType = "strong";

    //private static final ConcurrentHashMap<String, PriorityQueue<String>> operations = new ConcurrentHashMap<>();

    /**
     * TODO: Set the values of the following variables to the DNS names of your
     * three dataCenter instances. Be sure to match the regions with their DNS!
     * Do the same for the 3 Coordinators as well.
     */
    private static final String dataCenterUSE = "ec2-52-87-251-156.compute-1.amazonaws.com";
    private static final String dataCenterUSW = "ec2-54-86-99-37.compute-1.amazonaws.com";
    private static final String dataCenterSING = "ec2-54-86-154-243.compute-1.amazonaws.com";

    private static final String coordinatorUSE = "ec2-54-86-135-144.compute-1.amazonaws.com";
    private static final String coordinatorUSW = "ec2-52-90-173-2.compute-1.amazonaws.com";
    private static final String coordinatorSING = "ec2-52-23-199-49.compute-1.amazonaws.com";

    @Override
    public void start() {
        KeyValueLib.dataCenters.put(dataCenterUSE, 1);
        KeyValueLib.dataCenters.put(dataCenterUSW, 2);
        KeyValueLib.dataCenters.put(dataCenterSING, 3);
        KeyValueLib.coordinators.put(coordinatorUSE, 1);
        KeyValueLib.coordinators.put(coordinatorUSW, 2);
        KeyValueLib.coordinators.put(coordinatorSING, 3);
        final RouteMatcher routeMatcher = new RouteMatcher();
        final HttpServer server = vertx.createHttpServer();
        server.setAcceptBacklog(32767);
        server.setUsePooledBuffers(true);
        server.setReceiveBufferSize(4 * 1024);

        routeMatcher.get("/put", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final String value = map.get("value");
                final String timestamp = map.get("timestamp");
                final String forwarded = map.get("forward");
                final String forwardedRegion = map.get("region");
                final int hashVal = hash(key);

                Thread t = new Thread(new Runnable() {
                    public void run() {
                        try {
                            if (forwarded == null && consistencyType.equals("strong")) {
                                KeyValueLib.AHEAD(key, timestamp);
                                System.out.println("co ahead done");
                            }
                            if (region == hashVal) {
                                System.out.println("PUT: " + key + " " + timestamp + " " + region);
                                Thread t1 = new Thread(new Runnable() {
                                    public void run() {
                                        try {
                                            KeyValueLib.PUT(dataCenterUSE, key, value, timestamp, consistencyType);
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                        //System.out.println("co finish put us-east " + key + " " + timestamp);
                                    }
                                });
                                t1.start();
                                Thread t2 = new Thread(new Runnable() {
                                    public void run() {
                                        try {
                                            KeyValueLib.PUT(dataCenterUSW, key, value, timestamp, consistencyType);
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                        //System.out.println("co finish put us-west " + key + " " + timestamp);
                                    }
                                });
                                t2.start();
                                Thread t3 = new Thread(new Runnable() {
                                    public void run() {
                                        try {
                                            KeyValueLib.PUT(dataCenterSING, key, value, timestamp, consistencyType);
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                        //System.out.println("co finish put sing " + key + " " + timestamp);
                                    }
                                });
                                t3.start();
                                if (consistencyType.equals("strong")) {
                                    t1.join();
                                    t2.join();
                                    t3.join();
                                    KeyValueLib.COMPLETE(key, timestamp);
                                    //System.out.println("co complete done");
                                }
                            } else {
                                System.out.println("FORWARD: " + key + " " + timestamp + " " + region);
                                KeyValueLib.FORWARD(getDNSByRegion("", hashVal), key, value, timestamp);
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });
                t.start();
                req.response().end(); // Do not remove this
            }
        });

        routeMatcher.get("/get", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final String timestamp = map.get("timestamp");

                Thread t = new Thread(new Runnable() {
                    public void run() {
                        String response = "0";
                        try {
                            //System.out.println("GET: " + key + " " + timestamp + " " + region);
                            response = KeyValueLib.GET(getDNSByRegion("DC", region), key, timestamp, consistencyType);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        if (response.isEmpty()) {
                            response = "0";
                        }
                        req.response().end(response);
                    }

                });
                t.start();
            }
        });
        /* This endpoint is used by the grader to change the consistency level */
        routeMatcher.get("/consistency", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                consistencyType = map.get("consistency");
                req.response().end();
            }
        });
        routeMatcher.noMatch(new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                req.response().putHeader("Content-Type", "text/html");
                String response = "Not found.";
                req.response().putHeader("Content-Length",
                        String.valueOf(response.length()));
                req.response().end(response);
                req.response().close();
            }
        });
        server.requestHandler(routeMatcher);
        server.listen(8080);
    }

    private static String getDNSByRegion(String type, int n) {
        if (type.equals("DC")) {
            for (Map.Entry<String, Integer> entry : KeyValueLib.dataCenters.entrySet()) {
                if (!entry.getValue().equals(n)) continue;
                return entry.getKey();
            }
            System.out.println("No dataCenter Found!");
        } else {
            for (Map.Entry<String, Integer> entry : KeyValueLib.coordinators.entrySet()) {
                if (!entry.getValue().equals(n)) continue;
                return entry.getKey();
            }
            System.out.println("No Coordinator Found!");
        }
        return "null";
    }

    private static int hash(String input) {
        int val = 0;
        int letter;
        for (int i = 0; i < input.length(); i++) {
            letter = (Character.getNumericValue(input.toLowerCase().charAt(i)) - 9) % 3;
            if (i == input.length() - 1) {
                val = ((val + letter) % 3) % 3;
            } else {
                val = (((val + letter) % 3) * (26 % 3)) % 3;
            }
        }
        if (val == 0) {
            val = 3;
        }
        return Math.abs(val);
    }

}
