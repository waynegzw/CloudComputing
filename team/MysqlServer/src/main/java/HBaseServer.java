/**
 * Created by xgy on 16/03/16.
 */
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.cache.DirectBufferCache;
import io.undertow.util.Headers;
//import org.apache.commons.dbcp2.BasicDataSource;
import javax.sql.DataSource;
import java.io.PrintStream;
import java.sql.*;
import org.apache.hive.jdbc.HiveDriver;


public class HBaseServer {
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "default";
    private static final String URL = "jdbc:hive2://ip-172-31-17-211.ec2.internal:10000/" + DB_NAME;
    //private static final String URL = "jdbc:mysql://localhost/" + DB_NAME + "?useSSL=false";
    //private static final String URL1 = "jdbc:mysql://ec2-52-91-69-10.compute-1.amazonaws.com/" + DB_NAME + "?useSSL=false";
    //private static final String URL2 = "jdbc:mysql://ec2-54-164-12-244.compute-1.amazonaws.com/" + DB_NAME + "?useSSL=false";
    private static final String COMMA_DELIMITER = "\t";
    private static final String DB_USER = "ubuntu";
    private static final String DB_USER_LB = "lb";
    private static final String DB_PWD = "teamproject";
    private static final String DB_PWD_LB = "iamlb";
//    private static final BasicDataSource dataSource = new BasicDataSource();
//    private static final BasicDataSource dataSource1 = new BasicDataSource();
//    private static final BasicDataSource dataSource2 = new BasicDataSource();
    private static Connection con;

    private static String INFO = "YouKnowNothingJonSnow,9801-2388-2949\n";

    private static String SQL =
            "SELECT rowkey, content FROM twitter_tb " +
                    "WHERE rowkey = ? ";

    private static PrintStream out2;
    private static int offset = 0;

    public static void main(final String[] args) {
        try {
            initializeConnection();
            out2 = new PrintStream(System.out, true, "UTF-8");

            Undertow server = Undertow.builder()
                    .addHttpListener(80, args[0])
//                    .setBufferSize(1024*16).setDirectBuffers(true)
                    .setHandler(new HttpHandler() {
                        @Override
                        public void handleRequest(HttpServerExchange exchange) throws Exception {
                            String req = exchange.getQueryString();
                            if (req.length() == 0) {
                                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                                exchange.getResponseSender().send("OK");
                            }
                            else {
                                if (exchange.isInIoThread()) {
                                    exchange.dispatch(this);
                                    return;
                                }
                                StringBuilder result = getResult(exchange);
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

    private static StringBuilder getResult(HttpServerExchange exchange) throws SQLException {

        StringBuilder result = new StringBuilder(INFO);
        String uid = exchange.getQueryParameters().get("userid").getFirst();
        String tag = exchange.getQueryParameters().get("hashtag").getFirst();

        try (
                //Connection conn = dataSource.getConnection();
//                PreparedStatement preparedStmt = conn.prepareStatement(SQL)
                PreparedStatement preparedStmt = con.prepareStatement(SQL)
        ) {
            preparedStmt.setString(1, uid+"_"+tag);
//            preparedStmt.setString(2, tag);

            try (ResultSet rs = preparedStmt.executeQuery()) {
                while (rs.next()) {
//                    result.append(rs.getString("score")).append(":");
//                    result.append(rs.getString("ttime")).append(":");
//                    result.append(rs.getString("twitter_id")).append(":");
                    result.append(rs.getString("content")).append("\n");
                }
            }
        }
        result.append("\n");

        return result;

    }


    private static void initializeConnection() throws ClassNotFoundException, SQLException {
        String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        con = DriverManager.getConnection(URL, "", "");
//        dataSource.setDriverClassName(JDBC_DRIVER);
//        dataSource.setUrl(URL);
//        dataSource.setUsername(DB_USER);
//        dataSource.setPassword(DB_PWD);
//        dataSource.setInitialSize(50);
//        dataSource.setMinIdle(3);
//        dataSource.setMaxTotal(200);
//        dataSource2.setDriverClassName(JDBC_DRIVER);
//        dataSource2.setUrl(URL2);
//        dataSource2.setUsername(DB_USER_LB);
//        dataSource2.setPassword(DB_PWD_LB);
//        dataSource2.setInitialSize(100);
//        dataSource2.setMinIdle(3);
//        dataSource2.setMaxTotal(200);
    }
}
