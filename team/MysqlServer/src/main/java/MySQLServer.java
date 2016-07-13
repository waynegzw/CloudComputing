/**
 * Created by xgy on 12/03/16.
 */

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;


public class MySQLServer {
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "myisam";
    private static final String URL = "jdbc:mysql://localhost/" + DB_NAME + "?useSSL=false";
    private static final String URL1 = "jdbc:mysql://ec2-54-164-85-40.compute-1.amazonaws.com/" + DB_NAME + "?useSSL=false";
    private static final String URL2 = "jdbc:mysql://ec2-54-174-176-136.compute-1.amazonaws.com/" + DB_NAME + "?useSSL=false";
    private static final String URL3 = "jdbc:mysql://ec2-52-23-222-133.compute-1.amazonaws.com/" + DB_NAME + "?useSSL=false";
    private static final String URL4 = "jdbc:mysql://ec2-54-164-153-89.compute-1.amazonaws.com/" + DB_NAME + "?useSSL=false";
    private static final String URL5 = "jdbc:mysql://ec2-54-164-147-207.compute-1.amazonaws.com/" + DB_NAME + "?useSSL=false";
    private static final String COMMA_DELIMITER = "\t";
    private static final String DB_USER = "ubuntu";
    private static final String DB_USER_LB = "lb";
    private static final String DB_PWD = "teamproject";
    private static final String DB_PWD_LB = "iamlb";
    private static final BasicDataSource dataSource1 = new BasicDataSource();
    private static final BasicDataSource dataSource2 = new BasicDataSource();
    private static final BasicDataSource dataSource3 = new BasicDataSource();
    private static final BasicDataSource dataSource4 = new BasicDataSource();
    private static final BasicDataSource dataSourceLocal = new BasicDataSource();

    private static String INFO = "YouKnowNothingJonSnow,9801-2388-2949\n";

    private static String SQL =
                    "SELECT DISTINCT score, ttime, twitter_id, content FROM twitter_tb " +
                    "WHERE user_id = ? AND hashtag = ? collate utf8mb4_bin " +
                    "ORDER BY score DESC, ttime ASC, twitter_id ASC";

    private static PrintStream out2;
    static Integer offset = 0;

    public static void main(final String[] args) {
        try {
            initializeConnection();
            out2 = new PrintStream(System.out, true, "UTF-8");


            Undertow server = Undertow.builder()
                    .addHttpListener(80, "ec2-52-71-253-148.compute-1.amazonaws.com")
                    .setHandler(new HttpHandler() {
                        @Override
                        public void handleRequest(HttpServerExchange exchange) throws Exception {
                            String req = exchange.getQueryString();
                            //System.out.println(req.length());
                            if (req.length() == 0) {
                                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                                exchange.getResponseSender().send("OK");
                            }
                            else {
                                if (exchange.isInIoThread()) {
                                    exchange.dispatch(this);
                                    return;
                                }
                                offset %= 5;
                                StringBuilder result = getResult(exchange, offset);
                                offset += 1;
                                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8");
                                exchange.getResponseSender().send(result.toString());
                            }
                        }
                    }).build();
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static StringBuilder getResult(HttpServerExchange exchange, Integer offset) throws SQLException {

        StringBuilder result = new StringBuilder(INFO);
        String uid = exchange.getQueryParameters().get("userid").getFirst();
        String tag = exchange.getQueryParameters().get("hashtag").getFirst();
        BasicDataSource ds;
        if (offset == 0) ds = dataSource1;
        else if (offset == 1) ds = dataSource2;
        else if (offset == 2) ds = dataSource3;
        else if (offset == 3) ds = dataSource4;
        else if (offset == 4) ds = dataSourceLocal;
        else {
            ds = dataSource1;
            System.out.println(offset);
        }
        //System.out.println(ds.getUrl());
//
//        if (offset == 0) {
//            ds = dataSource1;
//        }
//        else {
//            ds = dataSource2;
//        }

        try (
                Connection conn = ds.getConnection();
                PreparedStatement preparedStmt = conn.prepareStatement(SQL)
        ) {
            preparedStmt.setString(1, uid);
            preparedStmt.setString(2, tag);

            try (ResultSet rs = preparedStmt.executeQuery()) {
                while (rs.next()) {
                    result.append(rs.getString("score")).append(":");
                    result.append(rs.getString("ttime")).append(":");
                    result.append(rs.getString("twitter_id")).append(":");
                    result.append(rs.getString("content")).append("\n");
                }
            }
        }
        result.append("\n");

        return result;

    }


    private static void initializeConnection() throws ClassNotFoundException, SQLException {
        dataSource1.setDriverClassName(JDBC_DRIVER);
        dataSource1.setUrl(URL1);
        dataSource1.setUsername(DB_USER_LB);
        dataSource1.setPassword(DB_PWD_LB);
        dataSource1.setInitialSize(50);
        dataSource1.setMinIdle(3);
        dataSource1.setMaxTotal(400);

        dataSource2.setDriverClassName(JDBC_DRIVER);
        dataSource2.setUrl(URL2);
        dataSource2.setUsername(DB_USER_LB);
        dataSource2.setPassword(DB_PWD_LB);
        dataSource2.setInitialSize(50);
        dataSource2.setMinIdle(3);
        dataSource2.setMaxTotal(400);

        dataSource3.setDriverClassName(JDBC_DRIVER);
        dataSource3.setUrl(URL3);
        dataSource3.setUsername(DB_USER_LB);
        dataSource3.setPassword(DB_PWD_LB);
        dataSource3.setInitialSize(50);
        dataSource3.setMinIdle(3);
        dataSource3.setMaxTotal(400);

        dataSource4.setDriverClassName(JDBC_DRIVER);
        dataSource4.setUrl(URL4);
        dataSource4.setUsername(DB_USER_LB);
        dataSource4.setPassword(DB_PWD_LB);
        dataSource4.setInitialSize(50);
        dataSource4.setMinIdle(3);
        dataSource4.setMaxTotal(400);

        dataSourceLocal.setDriverClassName(JDBC_DRIVER);
        dataSourceLocal.setUrl(URL5);
        dataSourceLocal.setUsername(DB_USER_LB);
        dataSourceLocal.setPassword(DB_PWD_LB);
        dataSourceLocal.setInitialSize(50);
        dataSourceLocal.setMinIdle(3);
        dataSourceLocal.setMaxTotal(400);
    }
}
