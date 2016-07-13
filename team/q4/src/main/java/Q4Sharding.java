import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.apache.commons.dbcp2.BasicDataSource;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static io.undertow.Handlers.path;

/**
 * Created by xgy on 12/04/16.
 */
public class Q4Sharding {

    private final static int MAX_ENTRIES = 20000;
    private final static int L2_MAX_SIZE = 1000;

    final private static LinkedHashMap<String, Tweet> Q4Entries = new LinkedHashMap<String, Tweet>(MAX_ENTRIES + 5000) {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > MAX_ENTRIES;
        }
    };

    final private static ConcurrentHashMap<String, Tweet> Q4L2Entries = new ConcurrentHashMap<>(1500); //TODO

    final private static ConcurrentHashMap<String, AtomicInteger> sequence_lock = new ConcurrentHashMap<>(40000);

    final private static ConcurrentHashMap<String, Boolean> exist = new ConcurrentHashMap<>(40000);

    final private static ExecutorService BATCH_EXECUTOR = Executors.newSingleThreadExecutor();

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "phase2";
    private static final String DB_URL = "jdbc:mysql://localhost/" + DB_NAME + "?useSSL=false";
    private static final String COMMA_DELIMITER = "\t";
    private static final String DB_USER = "team";
    private static final String DB_PWD = "teamproject";
    private static final BasicDataSource dataSource = new BasicDataSource();

    private static final String SHARD_1 = "ip-172-31-26-64.ec2.internal";
    private static final String SHARD_2 = "ip-172-31-26-63.ec2.internal";
    private static final String SHARD_3 = "ip-172-31-26-67.ec2.internal";
    private static final String SHARD_4 = "ip-172-31-26-62.ec2.internal";
    private static final String SHARD_5 = "ip-172-31-26-65.ec2.internal";
    private static final String SHARD_6 = "ip-172-31-26-66.ec2.internal";

    private static String LOCAL_URL;
    private static int MODE = 0; /* MySQL SHARDING */

    private static String INFO = "YouKnowNothingJonSnow,9801-2388-2949\n";
    private static String Q1_INFO = "YouKnowNothingJonSnow,9801-2388-2949\n2016-04-13 ";

    private static long Q3_SLICE_1 = 186242840;
    private static long Q3_SLICE_2 = 383026777;
    private static long Q3_SLICE_3 = 637142424;
    private static long Q3_SLICE_4 = 1467051524;
    private static long Q3_SLICE_5 = 2366272589L;


    public static void main(final String[] args) {
        try {
            if (args.length == 2) {
                LOCAL_URL = args[0];
                if (args[1].equals("msharding")) {
                    MODE = 0;
                } else if (args[1].equals("mstandalone")) {
                    MODE = 1;
                } else if (args[1].equals("csharding")) {
                    MODE = 2;
                } else {
                    MODE = 3; /* C standalone */
                }

            } else if (args.length == 1) {
                LOCAL_URL = args[0];
            } else {
                System.out.println("No LOCAL_URL input");
            }

            System.out.println(LOCAL_URL);
            initializeConnection();

            Undertow server = Undertow.builder()
                    .addHttpListener(80, "0.0.0.0").setWorkerThreads(2000)
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
                                    String req = exchange.getQueryString();

                                    String[] tokens = req.split("=");
                                    String key = tokens[1].substring(0, tokens[1].length() - 8);
                                    String encryptedStr = tokens[2];
                                    String message = Decipher.deCipher(encryptedStr, key);
                                    String response = Q1_INFO + PittsburghTime.getTime() + "\n" + message + "\n";
                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                                    exchange.getResponseSender().send(response);
                                }
                            }).addPrefixPath("/q2", new HttpHandler() {
                                @Override
                                public void handleRequest(HttpServerExchange exchange) throws Exception {
                                    if (exchange.isInIoThread()) {
                                        exchange.dispatch(this);
                                        return;
                                    }

                                    String result = getQ2Result(exchange);
                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8");
                                    exchange.getResponseSender().send(result);
                                }
                            }).addPrefixPath("/q3", new HttpHandler() {
                                @Override
                                public void handleRequest(HttpServerExchange exchange) throws Exception {
                                    if (exchange.isInIoThread()) {
                                        exchange.dispatch(this);
                                        return;
                                    }

                                    String result = getQ3Result(exchange);
                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8");
                                    exchange.getResponseSender().send(result);
                                }
                            }).addPrefixPath("/q4", new HttpHandler() {
                                @Override
                                public void handleRequest(HttpServerExchange exchange) throws Exception {
                                    if (exchange.isInIoThread()) {
                                        exchange.dispatch(this);
                                        return;
                                    }

                                    Map<String, Deque<String>> parameter_map = exchange.getQueryParameters();
                                    String tid = parameter_map.get("tweetid").peek();
                                    String op = parameter_map.get("op").peek();

                                    String shard = sharding(tid);

                                    if (shard != null && MODE != 1 && MODE != 3) {
                                        transfer(exchange, shard, op);
                                    } else {
                                        if (MODE > 1) {
                                            Q4Handler(exchange);
                                        } else {
                                            MySQLQ4Handler(exchange);
                                        }
                                    }
                                }
                            }).addPrefixPath("/t", new HttpHandler() {
                                @Override
                                public void handleRequest(HttpServerExchange exchange) throws Exception {
                                    if (exchange.isInIoThread()) {
                                        exchange.dispatch(this);
                                        return;
                                    }
                                    if (MODE > 1) {
                                        Q4Handler(exchange);
                                    } else {
                                        MySQLQ4Handler(exchange);
                                    }
                                }
                            }).addPrefixPath("/truncate", new HttpHandler() {
                                @Override
                                public void handleRequest(HttpServerExchange exchange) throws Exception {
                                    if (exchange.isInIoThread()) {
                                        exchange.dispatch(this);
                                        return;
                                    }
                                    clear_store();
                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8");
                                    exchange.getResponseSender().send("truncated");
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

    private static void clear_store() {
        Q4Entries.clear();
        Q4L2Entries.clear();
        exist.clear();
        sequence_lock.clear();
        try (Connection conn = dataSource.getConnection()
        ) {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("truncate q4table ");
            System.out.println("q4table truncated");

            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


    private static String sharding(final String tweetid) {
        int hash = tweetid.hashCode() % 6; //TODO
        String result_url = null;
        switch (Math.abs(hash)) {
            case 0:
                result_url = SHARD_1;
                break;
            case 1:
                result_url = SHARD_2;
                break;
            case 2:
                result_url = SHARD_3;
                break;
            case 3:
                result_url = SHARD_4;
                break;
            case 4:
                result_url = SHARD_5;
                break;
            case 5:
                result_url = SHARD_6;
                break;
            default:
                result_url = LOCAL_URL;
        }

        return result_url.equals(LOCAL_URL) ? null : result_url;

    }

    private static String transfer(final HttpServerExchange exchange, final String shard, final String op) {


        StringBuilder response = new StringBuilder();
        try {
            String query = exchange.getQueryString();

//            System.out.println("forward " + query + "to "+ shard);
            URL url = new URL("http://" + shard + "/t?" + query);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();

            con.setRequestMethod("GET");
            int responseCode = con.getResponseCode();

            if (responseCode != 200) {
                System.out.println(responseCode);
            }

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));

            String line;

            while ((line = in.readLine()) != null) {
                response.append(line).append("\n");
            }
            in.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        if (op.equals("get")) {
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8");
            exchange.getResponseSender().send(response.toString());
        }
        if (op.equals("set")) {
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8");
            exchange.getResponseSender().send(INFO + "success\n");
        }

        return response.toString();
    }


    private static String MySQLQ4Handler(final HttpServerExchange exchange) throws SQLException {

        Map<String, Deque<String>> parameter_map = exchange.getQueryParameters();
        String tweetid = parameter_map.get("tweetid").peek();
        int seq = Integer.valueOf(parameter_map.get("seq").peek());

        String query = exchange.getQueryString();
        String payload = query.substring(query.indexOf("&payload=") + 9);

        /* DEFINE OP */
        String op = parameter_map.get("op").peek();

        AtomicInteger current_seq = sequence_lock.get(tweetid);
        if (current_seq == null) {
            if (seq != 1) {
                System.out.println("None 1 SET");
            }
            current_seq = new AtomicInteger(1);
            sequence_lock.put(tweetid, current_seq);
        }

        String result = null;
        if (op.equals("get")) {
            acquire_lock(current_seq, seq);

            result = getQ4ResultMySQL(tweetid, parameter_map);

            release_lock(current_seq);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8");
            exchange.getResponseSender().send(result);
        } else {

            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8");
            exchange.getResponseSender().send(INFO + "success\n");

            if (exist.containsKey(tweetid)) {
                acquire_lock(current_seq, seq);

                result = updateQ4Result(parameter_map, tweetid, payload);

                release_lock(current_seq);
            } else { /* INSERT */
                acquire_lock(current_seq, seq);
                exist.put(tweetid, true);

                result = insertQ4Result(parameter_map, tweetid, payload);

                release_lock(current_seq);
            }
        }
        return result;
    }

    private static String insertQ4Result(Map<String, Deque<String>> parameter_map, final String tweetid, final String payload) {

        String[] temp = parameter_map.get("fields").peek().split(",");
        ArrayList<String> fields = new ArrayList<>(Arrays.asList(temp));

        String[] payloads = payload.split(",");
        ArrayList<String> values = new ArrayList<>(Arrays.asList(payloads));

        if (fields.size() == values.size() + 1) {
            values.add("");
        }

        try {
            StringBuilder insert_fields = new StringBuilder("tweetid,");
            StringBuilder question_mark = new StringBuilder(tweetid + ", ");
            for (int i = 0; i < fields.size(); i++) {
                if (i == fields.size() - 1) {
                    insert_fields.append(fields.get(i));
                    question_mark.append("?");
                } else {
                    insert_fields.append(fields.get(i)).append(",");
                    question_mark.append("?,");
                }
            }

            String INSERT_SQL = "INSERT INTO q4table "
                    + "(" + insert_fields.toString() + ") VALUES "
                    + "(" + question_mark.toString() + ")";

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement insert_statement = conn.prepareStatement(INSERT_SQL);
            ) {
                for (int i = 0; i < fields.size(); i++) {
                    insert_statement.setString(i + 1, values.get(i));
                }
                insert_statement.executeUpdate();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return INFO + "success\n";
    }


    private static String updateQ4Result(Map<String, Deque<String>> parameter_map, final String tweetid, final String payload) {

        String[] temp = parameter_map.get("fields").peek().split(",");
        ArrayList<String> fields = new ArrayList<String>(Arrays.asList(temp));
        String[] payloads = payload.split(",");
        ArrayList<String> values = new ArrayList<String>(Arrays.asList(payloads));

        if (fields.size() == values.size() + 1) {
            values.add("");
        }

        if (fields.size() != values.size()) {
            System.err.println("Un-equal length of fields and payload! ");
        }

        StringBuilder up = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            if (i == fields.size() - 1) {
                up.append(fields.get(i)).append("=").append("?");
            } else {
                up.append(fields.get(i)).append("=").append("?,");
            }
        }

        String update_SQL = "UPDATE q4table SET " + up.toString()
                + " WHERE tweetid = " + tweetid;

        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(update_SQL);
        ) {
            for (int i = 0; i < fields.size(); i++) {
                pst.setString(i + 1, values.get(i));
            }
            int check = pst.executeUpdate();
            if (check == 0) {
                System.err.println("0 row affected");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return INFO + "success\n";
    }


    private static void Q4Handler(final HttpServerExchange exchange) throws SQLException {

        Map<String, Deque<String>> parameter_map = exchange.getQueryParameters();
        String tweetid = parameter_map.get("tweetid").peek();
//        int seq = Integer.valueOf(parameter_map.get("seq").peek());
        String query = exchange.getQueryString();
        String payload = query.substring(query.indexOf("&payload=") + 9);

        /* DEFINE OP */

        String op = parameter_map.get("op").peek();
        if (op.equals("get")) {
            String result = getQ4Result(tweetid, parameter_map);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8");
            exchange.getResponseSender().send(result);
        } else {
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8");
            exchange.getResponseSender().send(INFO + "success\n");
            String result = setQ4Result(parameter_map, payload);
        }

    }

    private static String getQ4ResultMySQL(String tweetid, Map<String, Deque<String>> parameter_map) {

        String field = parameter_map.get("fields").peek();
        String SQL = "SELECT " + field + " FROM q4table WHERE tweetid = ? LIMIT 1";

        StringBuilder result = new StringBuilder(INFO);
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStmt = conn.prepareStatement(SQL)
        ) {
            preparedStmt.setString(1, tweetid);

            try (ResultSet rs = preparedStmt.executeQuery()) {
                while (rs.next()) {
                    result.append(rs.getString(field)).append("\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result.toString();
    }


    private static String getQ4Result(final String tweetid, final Map<String, Deque<String>> parameter_map) {
        String field = parameter_map.get("fields").peek();
        int seq = Integer.valueOf(parameter_map.get("seq").peek());

        AtomicInteger current_seq = sequence_lock.get(tweetid);
        if (current_seq == null) {
            current_seq = new AtomicInteger(1);
            sequence_lock.put(tweetid, current_seq);
        }

        if (seq < current_seq.get()) return INFO;

        /* ACQUIRE LOCK*/
        acquire_lock(current_seq, seq);

        // get from main entries
//        Tweet getTweet = Q4Entries.get(tweetid);
//        if (getTweet == null || getTweet.getField(field) == null) {
//            //get from l2 entries
//            getTweet = Q4L2Entries.get(tweetid);
//            if (getTweet == null || getTweet.getField(field) == null) {
//                if (exist.containsKey(tweetid)) {
//                    // get from DB
//                    return getQ4ResultMySQL(tweetid, parameter_map);
//                }
//                release_lock(current_seq);
//                return INFO + "\n";
//            }
//        }

        Tweet getTweet = Q4Entries.get(tweetid);
        if (getTweet == null || getTweet.getField(field) == null) {
            //get from l2 entries
            getTweet = Q4L2Entries.get(tweetid);
            if (getTweet == null || getTweet.getField(field) == null) {
                if (exist.containsKey(tweetid)) {
                    // get from DB
                    String result = getQ4ResultMySQL(tweetid, parameter_map);
                    release_lock(current_seq);
                    return result;
                } else {
                    release_lock(current_seq);
                    return INFO + "\n";
                }
            }
        }


        String result = INFO + getTweet.getField(field) + "\n";

        /* RELEASE LOCK*/
        release_lock(current_seq);

        return result;

    }

    private static void insertFromCacheToDB(List<String> fields, List<String> values, final String tweetid) {


        try {
            StringBuilder insert_fields = new StringBuilder("tweetid,");
            StringBuilder question_mark = new StringBuilder(tweetid + ", ");
            for (int i = 0; i < fields.size(); i++) {
                if (i == fields.size() - 1) {
                    insert_fields.append(fields.get(i));
                    question_mark.append("?");
                } else {
                    insert_fields.append(fields.get(i)).append(",");
                    question_mark.append("?,");
                }
            }

            String INSERT_SQL = "INSERT INTO q4table "
                    + "(" + insert_fields.toString() + ") VALUES "
                    + "(" + question_mark.toString() + ")";

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement insert_statement = conn.prepareStatement(INSERT_SQL);
            ) {
                for (int i = 0; i < fields.size(); i++) {
                    insert_statement.setString(i + 1, values.get(i));
                }
                insert_statement.executeUpdate();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void updateFromCacheToDB(List<String> fields, List<String> values, final String tweetid) {


        StringBuilder up = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            if (i == fields.size() - 1) {
                up.append(fields.get(i)).append("=").append("?");
            } else {
                up.append(fields.get(i)).append("=").append("?,");
            }
        }

        String update_SQL = "UPDATE q4table SET " + up.toString()
                + " WHERE tweetid = " + tweetid;

        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(update_SQL);
        ) {
            for (int i = 0; i < fields.size(); i++) {
                pst.setString(i + 1, values.get(i));
            }
            int check = pst.executeUpdate();
            if (check == 0) {
                System.err.println("0 row affected");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private static String setQ4Result(final Map<String, Deque<String>> parameter_map, final String payload) {
        String[] fields = parameter_map.get("fields").peek().split(",");
        String[] tmp = payload.split(",");
        String[] values;
        if (tmp.length != fields.length) {
            values = new String[fields.length];
            System.arraycopy(tmp, 0, values, 0, tmp.length);
        } else {
            values = tmp;
        }

        String tweetid = parameter_map.get("tweetid").peek();

        int seq = Integer.valueOf(parameter_map.get("seq").peek());

        AtomicInteger current_seq = sequence_lock.get(tweetid);
        if (current_seq == null) {
            current_seq = new AtomicInteger(1);
            sequence_lock.put(tweetid, current_seq);
        }

        if (seq < current_seq.get()) {
            System.out.println("Seq < cureent_timestamp");
            return null;
        }

        /* ACQUIRE LOCK */
        acquire_lock(current_seq, seq);

        Tweet tweet = Q4Entries.get(tweetid);
        if (tweet != null) {
            // update tweet in main entries
            for (int i = 0; i < fields.length; i++) {
                tweet.putField(fields[i], values[i]);
            }
        } else { //TODO linked full?
            tweet = Q4L2Entries.get(tweetid);
            if (tweet != null) {
                // update tweet in L2 entries
                for (int i = 0; i < fields.length; i++) {
                    tweet.putField(fields[i], values[i]);
                }
            } else {
                Tweet eldest_tweet = null;
                synchronized (Q4Entries) {
                    if (Q4Entries.size() == MAX_ENTRIES) {
                        eldest_tweet = Q4Entries.entrySet().iterator().next().getValue();
                    }
                    tweet = new Tweet(tweetid);
                    for (int i = 0; i < fields.length; i++) {
                        tweet.putField(fields[i], values[i]);
                    }
                    Q4Entries.put(tweetid, tweet);
                }

                if (eldest_tweet != null && Q4Entries.size() == MAX_ENTRIES) {
                    BATCH_EXECUTOR.execute(new BatchHandler(Q4L2Entries, exist, dataSource));

                    Q4L2Entries.put(eldest_tweet.tweetid, eldest_tweet);
                }
            }
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
                } else {
                    synchronized (current_seq) {
                        current_seq.wait(500);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void release_lock(final AtomicInteger current_seq) {

        synchronized (current_seq) {
            current_seq.getAndIncrement();
            current_seq.notifyAll();
        }

    }

    private static String getQ2Result(HttpServerExchange exchange) throws SQLException {

        StringBuilder result = new StringBuilder(INFO);
        try {

            String uid = exchange.getQueryParameters().get("userid").getFirst();
            String tag = exchange.getQueryParameters().get("hashtag").getFirst();

            String Q2_SQL =
                    "SELECT content FROM q2table WHERE user_id = ? AND hashtag = ? collate utf8mb4_bin LIMIT 1";

            try (
                    Connection conn = dataSource.getConnection();
                    PreparedStatement preparedStmt = conn.prepareStatement(Q2_SQL)
            ) {
                preparedStmt.setString(1, uid);
                preparedStmt.setString(2, tag);

                try (ResultSet rs = preparedStmt.executeQuery()) {
                    while (rs.next()) {
                        result.append(rs.getString("content").replace("}F{", "\n"));
                    }
                }
            }
        } catch (Exception e) {

        }

        result.append("\n\n");

        return result.toString();

    }


    private static String getQ3Result(HttpServerExchange exchange) throws SQLException {

        StringBuilder result = new StringBuilder();
        String words = exchange.getQueryParameters().get("words").getFirst();
        try {

            String start_date = exchange.getQueryParameters().get("start_date").getFirst();
            String end_date = exchange.getQueryParameters().get("end_date").getFirst();
            String start_userid = exchange.getQueryParameters().get("start_userid").getFirst();
            String end_userid = exchange.getQueryParameters().get("end_userid").getFirst();

            String sql =
                    "SELECT content FROM " + getQ3TableName(start_userid) +
                            " WHERE user_id BETWEEN ? AND ? AND date BETWEEN ? AND ? ";

            try (
                    Connection conn = dataSource.getConnection();
                    PreparedStatement preparedStmt = conn.prepareStatement(sql)
            ) {
                preparedStmt.setString(1, (start_userid));
                preparedStmt.setString(2, (end_userid));
                preparedStmt.setString(3, start_date);
                preparedStmt.setString(4, end_date);

                try (ResultSet rs = preparedStmt.executeQuery()) {
                    while (rs.next()) {
                        result.append(rs.getString("content"));
                    }
                }
            }

            return INFO + Q3WorldCount.worldCount(result.toString(), words);
        } catch (Exception e) {
            if (words == null || words.equals("")) {
                return INFO + "\n";
            } else {
                return INFO + Q3WorldCount.worldCount(result.toString(), words);
            }
        }

    }

    private static String getQ3TableName(String start_userid) throws Exception {
        Long start = Long.parseLong(start_userid);
        if (start < Q3_SLICE_1)
            return "q3table1";
        else if (start < Q3_SLICE_2)
            return "q3table2";
        else if (start < Q3_SLICE_3)
            return "q3table3";
        else if (start < Q3_SLICE_4)
            return "q3table4";
        else if (start < Q3_SLICE_5)
            return "q3table5";
        else
            return "q3table6";
    }


    private static void initializeConnection() throws ClassNotFoundException, SQLException {
        dataSource.setDriverClassName(JDBC_DRIVER);
        dataSource.setUrl(DB_URL);
        dataSource.setUsername(DB_USER);
        dataSource.setPassword(DB_PWD);
        dataSource.setInitialSize(20);
        dataSource.setMinIdle(3);
        dataSource.setMaxTotal(100);


        try (Connection conn = dataSource.getConnection()
        ) {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("truncate q4table ");
            System.out.println("q4table truncated");
            stmt.close();
        }

    }


    private static class BatchHandler implements Runnable {
        private ConcurrentHashMap<String, Tweet> L2Entries;
        private ConcurrentHashMap<String, Boolean> exist;
        private BasicDataSource dataSource;


        public BatchHandler(ConcurrentHashMap<String, Tweet> L2Entries,
                            ConcurrentHashMap<String, Boolean> exist, BasicDataSource dataSource) {
            this.dataSource = dataSource;
            this.L2Entries = L2Entries;
            this.exist = exist;
        }

        @Override
        public void run() {

            if (L2Entries.size() < L2_MAX_SIZE) {
                return;
            }

            Statement stmt = null;
            try (Connection conn = dataSource.getConnection();
            ) {
                Iterator<Tweet> itr = L2Entries.values().iterator();
                stmt = conn.createStatement();

                while (itr.hasNext()) {
                    Tweet t = itr.next();
                    addToBatch(t, stmt);
                }
                stmt.executeBatch();

                L2Entries.clear();

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (stmt != null) try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        private void addToBatch(Tweet t, Statement stmt) throws SQLException {

            if (exist.containsKey(t.tweetid)) {
                stmt.addBatch(t.getUpdateSQL());
            } else {
                stmt.addBatch(t.getInsertSQL());

                exist.put(t.tweetid, true);
            }
        }

    }
}
