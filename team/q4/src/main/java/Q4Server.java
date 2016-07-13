import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.apache.commons.dbcp2.BasicDataSource;

import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.undertow.Handlers.path;
import static io.undertow.Handlers.trace;


/**
 * Created by xgy on 11/04/16.
 */
public class Q4Server {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "phase3";
    private static final String URL = "jdbc:mysql://localhost/" + DB_NAME + "?useSSL=false";
    private static final String COMMA_DELIMITER = "\t";
    private static final String DB_USER = "team";
    private static final String DB_PWD = "teamproject";
    private static final BasicDataSource dataSource = new BasicDataSource();


    private static ConcurrentHashMap<String, AtomicInteger> sequence_lock = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Boolean> exist = new ConcurrentHashMap<>();

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
                                    String result = Q4Handler(exchange);

                                }
                            })
                    ).build();
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static String Q4Handler(final HttpServerExchange exchange) throws SQLException {

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

            result = getQ4Result(String.valueOf(tweetid), parameter_map);

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
                exist.put(tweetid, true);
                acquire_lock(current_seq, seq);

                result = setQ4Result(parameter_map, tweetid, payload);

                release_lock(current_seq);
            }
        }
        return result;
    }

    private static void acquire_lock(final AtomicInteger current_seq, final int seq) {
        int count = 0;
        try {
            while (true) {
                count++;
                if (current_seq.get() == seq) {
                    break;
                } else
                //  if (seq - current_seq.get() > 2 || count > 5000000)
                {
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

        synchronized (current_seq) {
            current_seq.getAndIncrement();
            current_seq.notifyAll();
        }

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

    private static String getQ4Result(String tweetid, Map<String, Deque<String>> parameter_map) {

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

    private static String setQ4Result(Map<String, Deque<String>> parameter_map, final String tweetid, final String payload) {

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
            /* TODO: CANT set table name by this */
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
        dataSource.setUrl(URL);
        dataSource.setUsername(DB_USER);
        dataSource.setPassword(DB_PWD);
        dataSource.setInitialSize(20);
        dataSource.setMinIdle(3);
        dataSource.setMaxTotal(200);
    }
}

