import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

public class KeyValueStore extends Verticle {

    /* TODO: Add code to implement your backend storage */
    private static final ConcurrentHashMap<String, PriorityBlockingQueue<String>> operations = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, String[]> data = new ConcurrentHashMap<>();

    @Override
    public void start() {
        final KeyValueStore keyValueStore = new KeyValueStore();
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
                        final String consistency = map.get("consistency");
                        Integer region = Integer.parseInt(map.get("region"));

                        final String timestamp = map.get("timestamp");

                        Thread t = new Thread(new Runnable() {
                            public void run() {
                                if (consistency.equals("strong")) {
                                    acquire_lock(key, timestamp);
                                    // System.out.println("dc start put " + key + " " + timestamp);
                                    String[] unit = {value, timestamp};
                                    data.put(key, unit);
                                    //System.out.println("dc finish put " + key + " " + timestamp);
                                } else {
                                    synchronized (data) {
                                        if (!data.containsKey(key) || Long.parseLong(data.get(key)[1]) < Long.parseLong(timestamp)) {
                                            //System.out.println("dc start put " + key + " " + timestamp);
                                            String[] unit = {value, timestamp};
                                            data.put(key, unit);
                                            //System.out.println("dc finish put " + key + " " + timestamp);
                                        }
                                    }
                                }
                                String response = "stored";
                                req.response().putHeader("Content-Type", "text/plain");
                                req.response().putHeader("Content-Length",
                                        String.valueOf(response.length()));
                                req.response().end(response);
                                req.response().close();
                            }
                        }

                        );
                        t.start();
                    }
                }

        );
        routeMatcher.get("/get", new Handler<HttpServerRequest>()

                {
                    @Override
                    public void handle(final HttpServerRequest req) {
                        MultiMap map = req.params();
                        final String key = map.get("key");
                        final String consistency = map.get("consistency");
                        final String timestamp = map.get("timestamp");

				/* TODO: Add code here to handle the get request
                     Remember that you may need to do some locking for this */
                        if (consistency.equals("strong")) {
                            operations.putIfAbsent(key, new PriorityBlockingQueue<String>());
                            //System.out.println("put " + timestamp + " into queue");
                            operations.get(key).offer(timestamp);
                        }

                        Thread t = new Thread(new Runnable() {
                            public void run() {
                                if (consistency.equals("strong")) acquire_lock(key, timestamp);
                                String response = "";
                                if (data.containsKey(key)) {
                                    response = data.get(key)[0];
                                }
                                if (consistency.equals("strong")) release_lock(key);
                                req.response().putHeader("Content-Type", "text/plain");
                                if (response != null)
                                    req.response().putHeader("Content-Length",
                                            String.valueOf(response.length()));
                                req.response().end(response);
                                req.response().close();
                            }
                        });
                        t.start();
                    }
                }

        );
        // Clears this stored keys. Do not change this
        routeMatcher.get("/reset", new Handler<HttpServerRequest>()

                {
                    @Override
                    public void handle(final HttpServerRequest req) {
                        data.clear();
                        req.response().putHeader("Content-Type", "text/plain");
                        req.response().end();
                        req.response().close();
                    }
                }

        );
        // Handler for when the AHEAD is called
        routeMatcher.get("/ahead", new Handler<HttpServerRequest>()

                {
                    @Override
                    public void handle(final HttpServerRequest req) {
                        MultiMap map = req.params();
                        String key = map.get("key");
                        final String timestamp = map.get("timestamp");
                        //System.out.println("dc ahead " + key + " " + timestamp);

                        operations.putIfAbsent(key, new PriorityBlockingQueue<String>());
                        System.out.println("put " + timestamp + " into queue");
                        operations.get(key).offer(timestamp);

                        req.response().putHeader("Content-Type", "text/plain");
                        req.response().end("AHEAD");
                        req.response().close();
                    }
                }

        );
        // Handler for when the COMPLETE is called
        routeMatcher.get("/complete", new Handler<HttpServerRequest>()

                {
                    @Override
                    public void handle(final HttpServerRequest req) {
                        MultiMap map = req.params();
                        String key = map.get("key");
                        final String timestamp = map.get("timestamp");
                        //System.out.println("dc complete " + key + " " + timestamp);
                        release_lock(key);
                        req.response().putHeader("Content-Type", "text/plain");
                        req.response().end("COMPLETE");
                        req.response().close();
                    }
                }

        );
        routeMatcher.noMatch(new Handler<HttpServerRequest>()

                             {
                                 @Override
                                 public void handle(final HttpServerRequest req) {
                                     req.response().putHeader("Content-Type", "text/html");
                                     String response = "Not found.";
                                     req.response().putHeader("Content-Length",
                                             String.valueOf(response.length()));
                                     req.response().end(response);
                                     req.response().close();
                                 }
                             }

        );
        server.requestHandler(routeMatcher);
        server.listen(8080);
    }

    private static void acquire_lock(String key, String timestamp) {
        synchronized (operations.get(key)) {
            //System.out.println("dc locked " + key + " " + operations.get(key).peek());
            try {
                while (!operations.get(key).peek().equals(timestamp)) {
                    //System.out.println("dc wait " + key + " " + timestamp);
                    operations.get(key).wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void release_lock(String key) {
        synchronized (operations.get(key)) {
            //System.out.println("dc unlock " + key);
            operations.get(key).poll();
            operations.get(key).notifyAll();
        }
    }
}
