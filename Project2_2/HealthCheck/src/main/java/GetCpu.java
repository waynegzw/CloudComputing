import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by Zhangwei on 2/12/16.
 */
public class GetCpu implements Runnable {
    private final DataCenterInstance[] instances;
    private double[] cpus = new double[3];

    public GetCpu(DataCenterInstance[] instances, double[] cpus) {
        this.instances = instances;
        this.cpus = cpus;
    }

    public void run() {
        while (true) {
            try {
                Thread.sleep(20);
                String res;
                for (int i = 0; i < 3; i++) {
                    res = fetch(instances[i].getUrl() + ":8080/info/cpu");
                    cpus[i] = Double.parseDouble(res.substring(res.indexOf("<body>") + 6, res.indexOf("</body>")));
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Make an HTTP request to a given URL
     *
     * @param urlString The URL of the request
     * @return A string of the response from the HTTP GET.  This is identical
     * to what would be returned from using curl on the command line.
     */

    public static String fetch(String urlString) {
        String response = "";
        try {
            URL url = new URL(urlString);

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            //connection.setRequestMethod("GET");
            // Read all the text returned by the server
            if (connection.getResponseCode() != 200) {
                return response;
            }
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"));
            String str;
            // Read each line of "in" until done, adding each to "response"
            while ((str = in.readLine()) != null) {
                // str is one line of text readLine() strips newline characters
                response += str;
            }
            in.close();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
        return response;
    }
}
