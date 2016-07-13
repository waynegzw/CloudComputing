import com.microsoft.azure.management.compute.models.VirtualMachineSizeTypes;
import com.microsoft.azure.utility.ResourceContext;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by Zhangwei on 2/12/16.
 */
public class HealthCheck implements Runnable {
    private final DataCenterInstance[] instances;

    public HealthCheck(DataCenterInstance[] instances) {
        this.instances = instances;
    }

    public void run() {
        while (true) {
            try {
                Thread.sleep(1000);
                for (int i = 0; i < 3; i++) {
                    String dnsUrl = instances[i].getUrl();
                    if (!dnsUrl.isEmpty()) {
                        String url = dnsUrl + "/lookup/random";
                        if (!isHealthy(url)) {
                            instances[i].setUrl("");
                            System.out.println("set " + i + ": " +instances[i].getUrl());
                            String dataCenterDNS = CreateDataCenter();
                            //add new instance to instances
                            instances[i].setUrl("http://" + dataCenterDNS);
                            System.out.println("instance "+ i + ": " + instances[i].getUrl());
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Send 5 requests to the url, if non of them has a 200 respond code, then the instance is unhealthy
     *
     * @param urlString the url to send request ro
     * @return whether is healthy or not
     */
    public static boolean isHealthy(String urlString) {
        for (int i = 0; i < 5; i++) {
            try {
                URL url = new URL(urlString);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setConnectTimeout(1500);
                if (connection.getResponseCode() == 200) {
                    return true;
                }
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        }
        return false;
    }

    /**
     * Create a new data center.
     *
     * @return new data center DNS
     * @throws IOException
     * @throws InterruptedException
     */
    public static String CreateDataCenter() throws Exception {
        String dataCenterDNS = (new AzureVMApiDemo()).create();
        // Wait until Instance finishes initializing
        Thread.sleep(120000);
        return dataCenterDNS;
    }

}