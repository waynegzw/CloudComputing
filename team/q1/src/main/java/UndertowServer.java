import io.undertow.Undertow;
import io.undertow.util.Headers;

/**
 * Created by Zhangwei on 2/27/16.
 */
public class UndertowServer {

    public static void main(final String[] args) {
//        Calendar calendar = new GregorianCalendar();
//        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
//        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT-5"));
//        return dateFormat.format(calendar.getTime());
        String info = "YouKnowNothingJonSnow,9801-2388-2949\n2016-03-15 ";
        Undertow server = Undertow.builder()
                .addHttpListener(80, "0.0.0.0")
                //.addHttpListener(8081, "localhost")
                .setHandler(exchange -> {
                    String req = exchange.getQueryString();
//                    System.out.println(req);
                    if (req.length() == 0) {
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                        exchange.getResponseSender().send("OK");
                    } else {
                        try {
                            String[] tokens = req.split("=");
                            String key = tokens[1].substring(0, tokens[1].length() - 8);
                            String encryptedStr = tokens[2];
                            String message = Decipher.deCipher(encryptedStr, key);
                            String response = info + PittsburghTime.getTime() + "\n" + message + "\n";
                            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                            exchange.getResponseSender().send(response);
                        } catch (Exception ex) {
                            System.out.print(ex);
                        }
                    }
                }).build();
        server.start();
    }
}
