
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * Created by Zhangwei on 2/27/16.
 */
public class PittsburghTime {
    public static String getTime() {
        Calendar calendar = new GregorianCalendar();
        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT-4"));
        return dateFormat.format(calendar.getTime());
    }
}
