import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.apache.commons.dbcp2.BasicDataSource;

import java.io.PrintStream;
import java.sql.SQLException;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.undertow.Handlers.path;


/**
 * Created by xgy on 11/04/16.
 */
public class Q4Cache {

    private static LinkedHashMap<String, Tweet> Q4Entries = new LinkedHashMap<String, Tweet>();
//    private static ConcurrentHashMap<String, Tweet> Q4Entries = new ConcurrentHashMap<>();

    private static ConcurrentHashMap<String, AtomicInteger> sequence_lock = new ConcurrentHashMap<>();


    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "phase3";
    private static final String URL = "jdbc:mysql://localhost/" + DB_NAME + "?useSSL=false";
    private static final String COMMA_DELIMITER = "\t";
    private static final String DB_USER = "team";
    private static final String DB_PWD = "teamproject";
    private static final BasicDataSource dataSource = new BasicDataSource();


    private static String INFO = "YouKnowNothingJonSnow,9801-2388-2949\n";
    private static String Q1_INFO = "YouKnowNothingJonSnow,9801-2388-2949\n2016-04-13 ";

    private static long Q3_SLICE_1 = 186242840;
    private static long Q3_SLICE_2 = 383026777;
    private static long Q3_SLICE_3 = 637142424;
    private static long Q3_SLICE_4 = 1467051524;
    private static long Q3_SLICE_5 = 2366272589L;

    private static PrintStream out2;


    public static void main(final String[] args) {
        try {

            out2 = new PrintStream(System.out, true, "UTF-8");
            initializeConnection();

            Undertow server = Undertow.builder()
                    .addHttpListener(80, "0.0.0.0").setWorkerThreads(1000)
                    .setHandler(path().addPrefixPath("/", new HttpHandler() {
                                @Override
                                public void handleRequest(HttpServerExchange exchange) throws Exception {
                                    String req = exchange.getQueryString();
                                    if (req.length() == 0) {
                                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                                        exchange.getResponseSender().send("OK");
                                    }
                                }
                            }).addPrefixPath("/q1", new HttpHandler() {
                                @Override
                                public void handleRequest(HttpServerExchange exchange) throws Exception {
                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                                    exchange.getResponseSender().send("\n");
                                }
                            }).addPrefixPath("/q2", new HttpHandler() {
                                @Override
                                public void handleRequest(HttpServerExchange exchange) throws Exception {
                                    if (exchange.isInIoThread()) {
                                        exchange.dispatch(this);
                                        return;
                                    }

                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8");
                                    exchange.getResponseSender().send("\n");
                                }
                            }).addPrefixPath("/q3", new HttpHandler() {
                                @Override
                                public void handleRequest(HttpServerExchange exchange) throws Exception {
                                    if (exchange.isInIoThread()) {
                                        exchange.dispatch(this);
                                        return;
                                    }

                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8");
                                    exchange.getResponseSender().send("\n");
                                }
                            }).addPrefixPath("/q4", new HttpHandler() {
                                @Override
                                public void handleRequest(HttpServerExchange exchange) throws Exception {
                                    if (exchange.isInIoThread()) {
                                        exchange.dispatch(this);
                                        return;
                                    }
                                    String result = Q4Handler(exchange);
//                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8");
//                                    exchange.getResponseSender().send(result);
                                }
                            })
                    ).build();
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println(Q4Entries.size());
        }
    }

    private static String Q4Handler(final HttpServerExchange exchange) throws SQLException {

        Map<String, Deque<String>> parameter_map = exchange.getQueryParameters();
        String tweetid = parameter_map.get("tweetid").peek();
        int seq = Integer.valueOf(parameter_map.get("seq").peek());
        String query = exchange.getQueryString();
        String payload = query.substring(query.indexOf("&payload=") + 9);

        /* DEFINE OP */
        //TODO with cache
        String op = parameter_map.get("op").peek();

        if (op.equals("get")) {
            String result = getQ4Result(tweetid, parameter_map);

            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8");
            exchange.getResponseSender().send(result);
            return result;
        } else {

            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8");
            exchange.getResponseSender().send(INFO + "success\n");
            String result = setQ4Result(parameter_map, payload);

            return result;
        }

    }

    private static String getQ4Result(final String tweetid, final Map<String, Deque<String>> parameter_map) {
        String field = parameter_map.get("fields").peek();
        int seq = Integer.valueOf(parameter_map.get("seq").peek());

        AtomicInteger current_seq = sequence_lock.get(tweetid);
        if (current_seq == null) {
            //TODO If not start from 1
            if (seq != 1) {
                System.out.println("non 1 get request");
            }
            current_seq = new AtomicInteger(1);
            sequence_lock.put(tweetid, current_seq);
        }

        if (seq < current_seq.get()) return null;
        /* ACQUIRE LOCK*/
        acquire_lock(current_seq, seq);

        if (Q4Entries.get(tweetid) == null || Q4Entries.get(tweetid).getField(field) == null) {
            release_lock(current_seq);
            return INFO + "\n";
        }

//        System.out.println("GET id:" + tweetid + "  atomic:" + current_seq.get() + " seq: " + seq);

        String result = INFO + Q4Entries.get(tweetid).getField(field) + "\n";

        /* RELEASE LOCK*/
        release_lock(current_seq);

        return result;

    }

    private static String setQ4Result(final Map<String, Deque<String>> parameter_map, final String payload) {
        String[] fields = parameter_map.get("fields").peek().split(",");
        String[] tmp = payload.split(",");
        String[] values = new String[fields.length];
        System.arraycopy(tmp, 0, values, 0, tmp.length);
//        System.out.println(Arrays.toString(fields));
//        System.out.println(Arrays.toString(values));
//        System.out.println("field len: " + fields.length);
//        System.out.println("values len: " + values.length);
        String tweetid = parameter_map.get("tweetid").peek();

        int seq = Integer.valueOf(parameter_map.get("seq").peek());

        AtomicInteger current_seq = sequence_lock.get(tweetid);
        if (current_seq == null) {
            //TODO If not start from 1
            if (seq != 1) {
                System.out.println("None 1 SET");
            }
            current_seq = new AtomicInteger(1);
            sequence_lock.put(tweetid, current_seq);
        }

//        System.out.println("set id:" + tweetid + "Atomic: " + current_seq.get() + " seq: " + seq);

        if (seq < current_seq.get()) return null;
        /* ACQUIRE LOCK */
        acquire_lock(current_seq, seq);

        if (Q4Entries.containsKey(tweetid)) {
            for (int i = 0; i < fields.length; i++) {
                Q4Entries.get(tweetid).putField(fields[i], values[i]);
            }
        } else {
            Tweet tweet = new Tweet(tweetid);
            for (int i = 0; i < fields.length; i++) {
                tweet.putField(fields[i], values[i]);
            }
            Q4Entries.put(tweetid, tweet);
        }

        /* RELEASE LOCK */
        release_lock(current_seq);

        return INFO + "success\n";
    }

    private static void acquire_lock(final AtomicInteger current_seq, final int seq) {
        try {
            while (true) {
                if (current_seq.get() == seq) {
                    break;
                }
                else if (seq - current_seq.get() > 3) {
                    synchronized (current_seq) {
                        current_seq.wait(500);
//                        ++wait_count;
//                        if (wait_count > 10) return;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void release_lock(final AtomicInteger current_seq) {

//        synchronized (current_seq) {
            current_seq.getAndIncrement();

//            current_seq.notifyAll();
//            System.out.println("notify all");
//        }

    }

    private static void initializeConnection() throws ClassNotFoundException, SQLException {
        dataSource.setDriverClassName(JDBC_DRIVER);
        dataSource.setUrl(URL);
        dataSource.setUsername(DB_USER);
        dataSource.setPassword(DB_PWD);
        dataSource.setInitialSize(20);
        dataSource.setMinIdle(3);
        dataSource.setMaxTotal(100);
    }
}

