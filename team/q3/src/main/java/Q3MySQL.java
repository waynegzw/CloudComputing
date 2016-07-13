import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.apache.commons.dbcp2.BasicDataSource;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static io.undertow.Handlers.path;

/**
 * Created by Zhangwei on 3/22/16.
 */
public class Q3MySQL {
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "phase2";
    private static final String URL = "jdbc:mysql://localhost/" + DB_NAME + "?useSSL=false";
    private static final String COMMA_DELIMITER = "\t";
    private static final String DB_USER = "team";
    private static final String DB_PWD = "teamproject";
    private static final BasicDataSource dataSource = new BasicDataSource();


    private static String INFO = "YouKnowNothingJonSnow,9801-2388-2949\n";

    private static PrintStream out2;
    private static int offset = 0;

    public static void main(final String[] args) {
        try {
            initializeConnection();
            out2 = new PrintStream(System.out, true, "UTF-8");

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

                        }
                    }).addPrefixPath("/q2", new HttpHandler() {
                        @Override
                        public void handleRequest(HttpServerExchange exchange) throws Exception {

                        }
                    }).addPrefixPath("/q3", new HttpHandler() {
                        @Override
                        public void handleRequest(HttpServerExchange exchange) throws Exception {
                            String req = exchange.getQueryString();
                            if (req.length() == 0) {
                                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                                exchange.getResponseSender().send("OK");
                            } else {
                                if (exchange.isInIoThread()) {
                                    exchange.dispatch(this);
                                    return;
                                }
                                String result = getQuery3Result(exchange);
                                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8");
                                exchange.getResponseSender().send(result);
                            }
                        }
                    })).build();
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getQuery3Result(HttpServerExchange exchange) throws SQLException {

        StringBuilder result = new StringBuilder();
        String start_date = exchange.getQueryParameters().get("start_date").getFirst();
        String end_date = exchange.getQueryParameters().get("end_date").getFirst();
        String start_userid = exchange.getQueryParameters().get("start_userid").getFirst();
        String end_userid = exchange.getQueryParameters().get("end_userid").getFirst();
        String words = exchange.getQueryParameters().get("words").getFirst();
//
//        String sql =
//                "SELECT DISTINCT content FROM " + getTableName(start_userid) +
//                        " WHERE user_id BETWEEN '" + start_userid + "' AND '" + end_userid +
//                        "' AND ttime BETWEEN STR_TO_DATE('" + start_date + "', '%Y-%m-%d') " +
//                        "AND STR_TO_DATE('" + end_date + "', '%Y-%m-%d'); ";
//        String sql =
//                "SELECT DISTINCT content FROM " + getTableName(start_userid) +
//                        " WHERE user_id BETWEEN '" + start_userid + "' AND '" + end_userid +
//                        "' AND date BETWEEN STR_TO_DATE('" + start_date + "', '%Y-%m-%d') " +
//                        "AND STR_TO_DATE('" + end_date + "', '%Y-%m-%d'); ";

        String sql =
                "SELECT content FROM " + getTableName(start_userid) +
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

            System.out.println(preparedStmt.toString());
            try (ResultSet rs = preparedStmt.executeQuery()) {
                while (rs.next()) {
                    result.append(rs.getString("content"));
                }
            }
        }

        return INFO + Q3WorldCount.worldCount(result.toString(), words);

    }

    private static String getTableName(String start_userid) {
        Long start = Long.parseLong(start_userid);
        if (start < Long.parseLong("346970078"))
            return "table1";
        else if (start < Long.parseLong("1129715918"))
            return "table2";
//        else if (start < Long.parseLong("599412885"))
//            return "table3";
//        else if (start < Long.parseLong("1296636944"))
//            return "table4";
//        else if (start < Long.parseLong("2274669612"))
//            return "table5";
        else
            return "table3";
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
