import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;


/**
 * Created by Zhangwei on 2/11/16.
 */
public class Test {

    public static void main(String[] args) throws IOException {
        String url = "http://ec2-52-87-225-4.compute-1.amazonaws.com:8080/info/cpu";
        //System.out.println(fetch(url));
        String resp = "<!DOCTYPE html><html><head><title>MSB Data Center</title></head><body>0.00</body></html>";


        System.out.println(resp.substring(resp.indexOf("<body>") + 6, resp.indexOf("</body>")));


//        //String url = "http://www.google.com/search?q=httpClient";
//
//        HttpClient client = HttpClientBuilder.create().build();
//        HttpGet request = new HttpGet(url);
//
//        // add request header
//        //request.addHeader("User-Agent", USER_AGENT);
//        HttpResponse response = client.execute(request);
//
//        System.out.println("Response Code : "
//                + response.getStatusLine().getStatusCode());
//
//        BufferedReader rd = new BufferedReader(
//                new InputStreamReader(response.getEntity().getContent()));
//
//        StringBuffer result = new StringBuffer();
//        String line = "";
//        while ((line = rd.readLine()) != null) {
//            result.append(line);
//        }
//
//        System.out.println(line);
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
