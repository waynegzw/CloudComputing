import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by Zhangwei on 3/28/16.
 */
public class Q3HBase {
    /**
     * The private IP address of HBase master node.
     */
    private static String zkAddr = "172.31.17.211:";
    /**
     * The name of your HBase table.
     */
    private static String tableName = "tweetsdata";
    /**
     * HTable handler.
     */
    private static HTableInterface tweetsTable;
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

    private static String INFO = "YouKnowNothingJonSnow,9801-2388-2949\n";

    static {
        // Remember to set correct log level to avoid unnecessary output.
        logger.setLevel(Level.INFO);
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":60000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        try {
            conn = HConnectionManager.createConnection(conf);
            tweetsTable = conn.getTable(Bytes.toBytes(tableName));
            HTableDescriptor tableDesc = new HTableDescriptor("myTable");
            HColumnDescriptor cfDesc = new HColumnDescriptor("myCF");
            cfDesc.setPrefetchBlocksOnOpen(true);
            tableDesc.addFamily(cfDesc);
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        Undertow server = Undertow.builder()
                .addHttpListener(80, "0.0.0.0")
                .setHandler(new HttpHandler() {
                    @Override
                    public void handleRequest(final HttpServerExchange exchange) throws Exception {
                        if (exchange.isInIoThread()) {
                            exchange.dispatch(this);
                            return;
                        }
                        String result = getResult(exchange);
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8");
                        exchange.getResponseSender().send(result);
                    }
                }).build();
        server.start();
    }

    private static String getResult(HttpServerExchange exchange) throws IOException {
        String result = "";
        if (exchange != null) {
            String start_date = exchange.getQueryParameters().get("start_date").getFirst();
            String end_date = exchange.getQueryParameters().get("end_date").getFirst();
            String start_userid = exchange.getQueryParameters().get("start_userid").getFirst();
            String end_userid = exchange.getQueryParameters().get("end_userid").getFirst();
            String words = exchange.getQueryParameters().get("words").getFirst();


            byte[] prefix1 = Bytes.toBytes(start_userid);
            byte[] prefix2 = Bytes.toBytes(end_userid);
            Scan scan = new Scan(prefix1, prefix2);
            byte[] bCol = Bytes.toBytes("content");
            scan.addColumn(bColFamily, bCol);
            PrefixFilter prefixFilter1 = new PrefixFilter(prefix1);
            PrefixFilter prefixFilter2 = new PrefixFilter(prefix2);
            scan.setFilter(prefixFilter1).setFilter(prefixFilter2).setBatch(10);

            ResultScanner rs = tweetsTable.getScanner(scan);
            for (Result r = rs.next(); r != null; r = rs.next()) {
                //System.out.println(Bytes.toString(r.getValue(bColFamily, bCol)));
            }
            rs.close();

        }
        return result;
    }
}
