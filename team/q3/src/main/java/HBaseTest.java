import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.PrintStream;

import static io.undertow.Handlers.path;

/**
 * Created by Zhangwei on 3/28/16.
 */
public class HBaseTest {

    /**
     * The private IP address of HBase master node.
     */
    private static String zkAddr = "172.31.54.107";
    /**
     * The name of your HBase table.
     */
    private static String tableName = "wordcount";
    /**
     * HTable handler.
     */
    private static HTableInterface tweetsTable;
    private static HTable table;
    /**
     * HBase connection.
     */
    private static HConnection conn;
    /**
     * Byte representation of column family.
     */
    private static byte[] bColFamily = Bytes.toBytes("d");
    /**
     * Logger.
     */
    private final static Logger logger = Logger.getRootLogger();


    /**
     * Initialize HBase connection.
     *
     * @throws IOException
     */
    private static void initializeConnection()  {
        // Remember to set correct log level to avoid unnecessary output.
        try {
            logger.setLevel(Level.ERROR);
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.master", zkAddr + ":60000");
            conf.set("hbase.zookeeper.quorum", zkAddr);
            conf.set("hbase.zookeeper.property.clientport", "2181");
            if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
                System.out.print("HBase not configured!");
                return;
            }
            //conn = HConnectionManager.createConnection(conf);
            //tweetsTable = conn.getTable(Bytes.toBytes(tableName));
            table = new HTable(conf, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(final String[] args) {
        initializeConnection();

        try {

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

                                //        System.out.println("Start Query..");
//        //nfollow:22;back:7;retweet:7;
//                                String start_date = "2014-03-26";
//                                String end_date = "2014-04-17";
//                                String start_userid = "193076543";
//                                String end_userid = "193077019";
//                                String words = "follow,back,retweet";

                                String start_date = exchange.getQueryParameters().get("start_date").getFirst();
                                String end_date = exchange.getQueryParameters().get("end_date").getFirst();
                                String start_userid = exchange.getQueryParameters().get("start_userid").getFirst();
                                String end_userid = exchange.getQueryParameters().get("end_userid").getFirst();
                                String words = exchange.getQueryParameters().get("words").getFirst();
//
//        //follow:2;nlkegiveaway:2;one:3;
//        String start_date = "2014-04-13";
//        String end_date = "2014-04-14";
//        String start_userid = "2469822539";
//        String end_userid = "2469823657";
//        String words = "follow,nlkegiveaway,one";

                                String raw_result = demo(start_date, end_date, start_userid, end_userid);
                                String result = Q3WorldCount.worldCount(raw_result, words);
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
//    public static void main(String[] args) throws IOException {
//        initializeConnection();
//        System.out.println("Start Query..");
//        //nfollow:22;back:7;retweet:7;
////        String start_date = "2014-03-26";
////        String end_date = "2014-04-17";
////        String start_userid = "193076543";
////        String end_userid = "193077019";
////        String words = "follow,back,retweet";
//
//        //follow:2;nlkegiveaway:2;one:3;
//        String start_date = "2014-04-13";
//        String end_date = "2014-04-14";
//        String start_userid = "2469822539";
//        String end_userid = "2469823657";
//        String words = "follow,nlkegiveaway,one";
//
//        String text = demo(start_date, end_date, start_userid, end_userid);
//        System.out.println("text: " + text);
//        System.out.println(Q3WorldCount.worldCount(text, words));
//    }


    private static String demo(String start_date, String end_date, String start_userid, String end_userid) throws IOException {
        String result = "";

        byte[] prefix1 = Bytes.toBytes(start_userid);
        byte[] prefix2 = Bytes.toBytes(end_userid);
        Scan scan = new Scan(prefix1, prefix2);
        byte[] bCol1 = Bytes.toBytes("date");
        byte[] bCol2 = Bytes.toBytes("text");
        scan.addColumn(bColFamily, bCol1).addColumn(bColFamily, bCol2);
        //PrefixFilter prefixFilter1 = new PrefixFilter(prefix1);
        //PrefixFilter prefixFilter2 = new PrefixFilter(prefix2);
        Filter filter1 = new SingleColumnValueFilter(bColFamily, bCol1, CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(start_date));
        Filter filter2 = new SingleColumnValueFilter(bColFamily, bCol1, CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(end_date));
        FilterList filterList = new FilterList();
        //filterList.addFilter(prefixFilter1);
        //filterList.addFilter(prefixFilter2);
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);
        scan.setFilter(filterList);
        //scan.setBatch(10);


        ResultScanner rs = table.getScanner(scan);
        for (Result r = rs.next(); r != null; r = rs.next()) {
            result += Bytes.toString(r.getValue(bColFamily, bCol2));
        }
        rs.close();

        return result;
    }
}

