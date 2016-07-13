import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.PriorityQueue;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

public class Coordinator extends Verticle {

    //Default mode: sharding. Possible string values are "replication" and "sharding"
    private static String storageType = "sharding";

    /**
     * TODO: Set the values of the following variables to the DNS names of your
     * three dataCenter instances
     */
    private static final String dataCenter1 = "ec2-54-208-191-69.compute-1.amazonaws.com";
    private static final String dataCenter2 = "ec2-52-87-166-54.compute-1.amazonaws.com";
    private static final String dataCenter3 = "ec2-52-23-169-76.compute-1.amazonaws.com";
    // generate a queue for each key
    private static final ConcurrentHashMap<String, PriorityQueue<String>> operations = new ConcurrentHashMap<>();
    //private static int[] a = new int[3];

    @Override
    public void start() {
        //DO NOT MODIFY THIS
        KeyValueLib.dataCenters.put(dataCenter1, 1);
        KeyValueLib.dataCenters.put(dataCenter2, 2);
        KeyValueLib.dataCenters.put(dataCenter3, 3);
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
                //You may use the following timestamp for ordering requests
                final String timestamp = new Timestamp(System.currentTimeMillis()
                        + TimeZone.getTimeZone("EST").getRawOffset()).toString();
                final int hashVal = hash(key);
                // put operations into queue
                if (operations.get(key) == null) {
                    operations.put(key, new PriorityQueue<String>());
                }
                operations.get(key).offer(timestamp);
                Thread t = new Thread(new Runnable() {
                    public void run() {
                        try {
                            //lock the queue for the key
                            acquire_lock(key, timestamp);
                            //System.out.println("key: " + key + " hash: " + hashVal);
                            //put data
                            if (storageType.equals("replication")) {
                                KeyValueLib.PUT(dataCenter1, key, value);
                                KeyValueLib.PUT(dataCenter2, key, value);
                                KeyValueLib.PUT(dataCenter3, key, value);
                            } else {
                                switch (hashVal) {
                                    case 1:
                                        KeyValueLib.PUT(dataCenter1, key, value);
                                        //a[0]++;
                                        break;
                                    case 2:
                                        KeyValueLib.PUT(dataCenter2, key, value);
                                        //a[1]++;
                                        break;
                                    case 3:
                                        KeyValueLib.PUT(dataCenter3, key, value);
                                        //a[2]++;
                                        break;
                                }
                            }
                            //unlock
                            //System.out.println(Arrays.toString(a));
                            release_lock(key);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
                t.start();
                req.response().end(); //Do not remove this
            }
        });

        routeMatcher.get("/get", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final String loc = map.get("loc");
                //You may use the following timestamp for ordering requests
                final String timestamp = new Timestamp(System.currentTimeMillis()
                        + TimeZone.getTimeZone("EST").getRawOffset()).toString();
                final int hashVal = hash(key);
                if (operations.get(key) == null) {
                    operations.put(key, new PriorityQueue<String>());
                }
                operations.get(key).offer(timestamp);
                Thread t = new Thread(new Runnable() {
                    public void run() {
                        String res = "";
                        //get data
                        try {
                            acquire_lock(key, timestamp);
                            if (storageType.equals("replication")) {
                                switch (loc) {
                                    case "1":
                                        req.response().end(KeyValueLib.GET(dataCenter1, key));
                                        //System.out.println("get: " + loc + " " + key + ": " + KeyValueLib.GET(dataCenter1, key));
                                        break;
                                    case "2":
                                        req.response().end(KeyValueLib.GET(dataCenter2, key));
                                        //System.out.println("get: " + loc + " " + key + ": " + KeyValueLib.GET(dataCenter2, key));
                                        break;
                                    case "3":
                                        req.response().end(KeyValueLib.GET(dataCenter3, key));
                                        //System.out.println("get: " + loc + " " + key + ": " + KeyValueLib.GET(dataCenter3, key));
                                        break;
                                    default:
                                        req.response().end("0"); //Default response = 0
                                        break;
                                }
                            } else {
                                switch (hashVal) {
                                    case 1:
                                        res = KeyValueLib.GET(dataCenter1, key);
                                        //System.out.println("get: " + loc + " " + key + ": " + res);
                                        break;
                                    case 2:
                                        res = KeyValueLib.GET(dataCenter2, key);
                                        //System.out.println("get: " + loc + " " + key + ": " + res);
                                        break;
                                    case 3:
                                        res = KeyValueLib.GET(dataCenter3, key);
                                        //System.out.println("get: " + loc + " " + key + ": " + res);
                                        break;
                                }
                                if (res.isEmpty()) {
                                    res = "0";
                                }
                                if (!loc.isEmpty() && Integer.parseInt(loc) != hashVal) {
                                    req.response().end("0"); //Default response = 0
                                } else {
                                    req.response().end(res);
                                }
                            }
                            release_lock(key);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
                t.start();
            }
        });

        routeMatcher.get("/storage", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                storageType = map.get("storage");
                //This endpoint will be used by the auto-grader to set the
                //consistency type that your key-value store has to support.
                //You can initialize/re-initialize the required data structures here
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

    private static void acquire_lock(String key, String timestamp) {
        synchronized (operations.get(key)) {
            try {
                while (!operations.get(key).peek().equals(timestamp)) {
                    operations.get(key).wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void release_lock(String key) {
        synchronized (operations.get(key)) {
            operations.get(key).poll();
            operations.get(key).notifyAll();
        }
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
        //return input.charAt(0) % 3 == 0 ? 3 : input.charAt(0) % 3;
    }
}
