import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;


/**
 * Created by xgy on 27/02/16.
 */
public class VertxServer {

    public static void main(String[] args) {
        Test test = new Test();
        Decipher decipher = new Decipher();
        Vertx vertx = Vertx.vertx();
        HttpServer vServer = vertx.createHttpServer();
        Calendar calendar = new GregorianCalendar();
        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT-5"));
        String info = "YouKnowNothingJonSnow,9801-2388-2949\n" + "2016-02-28 ";

        vServer.requestHandler(req -> {
            if (req.uri().equals("/")) {
                req.response().end("Welcome");
            }
            else if (req.uri().startsWith("/q1")) {
                req.response().setChunked(true);
                String result = decipher.decipher(req.getParam("message"), req.getParam("key"));
                req.response().write(info + dateFormat.format(calendar.getTime()) + "\n" + result + "\n");
                req.response().end();
            }
        }).listen(80);
    }
}
