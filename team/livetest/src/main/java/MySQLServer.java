
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.math3.linear.SymmLQ;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static io.undertow.Handlers.path;

/**
 * Created by Zhangwei on 3/22/16.
 */
public class MySQLServer {
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "phase2";
    private static final String URL = "jdbc:mysql://localhost/" + DB_NAME + "?useSSL=false";
    private static final String COMMA_DELIMITER = "\t";
    private static final String DB_USER = "team";
    private static final String DB_PWD = "teamproject";
    private static final BasicDataSource dataSource = new BasicDataSource();


    private static String INFO = "YouKnowNothingJonSnow,9801-2388-2949\n";
    private static String Q1_INFO = "YouKnowNothingJonSnow,9801-2388-2949\n2016-04-06 ";

    private static long Q3_SLICE_1 = 186242840;
    private static long Q3_SLICE_2 = 383026777;
    private static long Q3_SLICE_3 = 637142424;
    private static long Q3_SLICE_4 = 1467051524;
    private static long Q3_SLICE_5 = 2366272589L;

    private static PrintStream out2;

    private static int offset = 0;

    public static void main(final String[] args) {
        try {

            out2 = new PrintStream(System.out, true, "UTF-8");
            initializeConnection();

            Undertow server = Undertow.builder()
                    .addHttpListener(80, "0.0.0.0")
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
                    })).build();
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
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

    private static String getQ2TableName(String start_userid) throws Exception {
        Long start = Long.parseLong(start_userid);
//        if (start < Q2_SLICE_1)
//            return "q2table1";
//        else if (start < Q2_SLICE_2)
//            return "q2table2";
//        else if (start < Q2_SLICE_3)
//            return "q2table3";
//        else if (start < Q2_SLICE_4)
//            return "q2table4";
//        else if (start < Q2_SLICE_5)
//            return "q2table5";
//        else if (start < Q2_SLICE_6)
//            return "q2table6";
//        else if (start < Q2_SLICE_7)
//            return "q2table7";
//        else if (start < Q2_SLICE_8)
//            return "q2table8";
//        else
//            return "q2table9";


        if (start < 1167615164L) {
            if (start < 345600649L) {
                if (start < 155871132L) {
                    return "q2table1";
                } else {
                    return "q2table2";
                }
            } else {
                if (start < 578786395L) {
                    return "q2table3";
                } else {
                    return "q2table4";
                }
            }
        } else {
            if (start < 1899368234L) {
                return "q2table5";
            } else if (start < 2370028848L) {
                return "q2table6";
            } else {
                return "q2table7";
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
        dataSource.setInitialSize(50);
        dataSource.setMinIdle(3);
        dataSource.setMaxTotal(200);
    }
}
