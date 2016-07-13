import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import javafx.scene.paint.Stop;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import static io.undertow.Handlers.path;

/**
 * Created by Zhangwei on 3/28/16.
 */
public class HBaseServer {

    /**
     * The private IP address of HBase master node.
     */
    private static String zkAddr = "172.31.16.253";
    /**
     * The name of your HBase table.
     */
    private static String q3tableName = "words";
    private static HTable q3table;
    private static HTable q3table2;

    private static String q2tableName = "tweetsdata";
    private static HTable q2table;

    private static byte[] bColFamily = Bytes.toBytes("d");
    /**
     * HBase connection.
     */
    private static HConnection conn;

    private final static Logger logger = Logger.getRootLogger();

    private static String INFO = "YouKnowNothingJonSnow,9801-2388-2949\n";
    private static String Q1_INFO = "YouKnowNothingJonSnow,9801-2388-2949\n2016-03-30 ";

    private static Integer offset = 0;

    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    /**
     * Initialize HBase connection.
     *
     * @throws IOException
     */

    static {
        // Remember to set correct log level to avoid unnecessary output.
        try {
            logger.setLevel(Level.ERROR);
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.master", zkAddr + ":60000");
            conf.set("hbase.zookeeper.quorum", zkAddr);
            conf.set("hbase.zookeeper.property.clientport", "2181");
            //conn = HConnectionManager.createConnection(conf);
            //tweetsTable = conn.getTable(Bytes.toBytes(tableName));
//            q2table = new HTable(conf, q2tableName);
            q3table = new HTable(conf, q3tableName);
            q3table2 = new HTable(conf, q3tableName);
            System.out.print("HBase connected!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(final String[] args) {
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
                            String req = exchange.getQueryString();
                            offset ^= 1;
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

    private static String getQ2Result(HttpServerExchange exchange) throws IOException {
        String result = "";
        if (exchange != null) {
            String uid = exchange.getQueryParameters().get("userid").getFirst();
            String tag = exchange.getQueryParameters().get("hashtag").getFirst();
            if (!uid.isEmpty() && !tag.isEmpty()) {
                String rowKey = uid + "_" + tag;
                Get get = new Get(Bytes.toBytes(rowKey));
                Result rs = q2table.get(get);
                byte[] bCol = Bytes.toBytes("content");
                get.addColumn(bColFamily, bCol);
                result = INFO + StringEscapeUtils.unescapeJava(Bytes.toString(rs.getValue(bColFamily, bCol)))
                        .replace("{{#FGF#}}", "\n") + "\n" + "\n";
                q2table.close();
            }
        }
        return result;
    }

    private static String getQ3Result(HttpServerExchange exchange) throws IOException, ParseException {

        HTable table = null;

        if (offset == 0) {
            table = q3table;
        } else {
            table = q3table2;
        }


        String start_date = exchange.getQueryParameters().get("start_date").getFirst();
        String end_date = exchange.getQueryParameters().get("end_date").getFirst();
        String start_userid = exchange.getQueryParameters().get("start_userid").getFirst();
        String end_userid = exchange.getQueryParameters().get("end_userid").getFirst();
        String words = exchange.getQueryParameters().get("words").getFirst();

        StringBuilder raw_result = new StringBuilder();

//        ArrayList<Get> gets = new ArrayList<Get>();
//        for (int i = Integer.parseInt(start_userid); i <= Integer.parseInt(end_userid); i++) {
//            gets.add(new Get(Bytes.toBytes(i)));
//        }

        String req = exchange.getQueryString();
        String start = start_userid;
        String end = end_userid;
        if (start_userid.length() < 10) {
            start = String.format("%10s", start_userid).replace(" ", "0");
        }
        if (end_userid.length() < 10) {
            end = String.format("%10d", Integer.parseInt(end_userid) + 1).replace(" ", "0");
        }
        Scan scan = new Scan(Bytes.toBytes(start), Bytes.toBytes(end));
        byte[] bCol = Bytes.toBytes("content");
        scan.addColumn(bColFamily, bCol);
        scan.setBatch(10);

        Stopwatch time1 = new Stopwatch();
        ResultScanner rs = table.getScanner(scan);
        System.out.println("scan time:" + time1.elapsedTime() + "\t" + req);

        Stopwatch time2 = new Stopwatch();
        for (Result r = rs.next(); r != null; r = rs.next()) {
            raw_result.append(Bytes.toString(r.getValue(bColFamily, bCol))).append(",");
        }
        rs.close();

        String tmp = getTwittee(start_date, end_date, raw_result.toString());

        System.out.println("get twitter:" + time2.elapsedTime() + "\t" + req);
//        System.out.println(raw_result.toString());
        Stopwatch time3 = new Stopwatch();
//        String result = Q3WorldCount.worldCount(raw_result.toString(), words);
        String result = Q3WorldCount.worldCount(tmp, words);
        System.out.println("wordcount time:" + time3.elapsedTime() + "\t" + req);
        return INFO + result;
    }

    private static String getTwittee(String start, String end, String content) throws ParseException {
        StringBuilder result = new StringBuilder();
        String[] parts = content.split(",");
        Date startDate = formatter.parse(start);
        Date endDate = formatter.parse(end);

        if (parts.length > 0) {
            for (int i = 0; i < parts.length; i++) {
                String[] temp = parts[i].split(":");
                try {
                    Date date = formatter.parse(temp[0]);

                    if (!(date.before(startDate) || date.after(endDate))) {
                        result.append(temp[1]);
                        result.append(" ");
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
        return result.toString();
    }
}
