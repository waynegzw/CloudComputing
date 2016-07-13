
import com.google.gson.Gson;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by shenanqi on 2/28/16.
 * This class is used to get twitter content, id and date
 */
public class Twitter {
    public String id_str;
    public String created_at;
    public String text;
    public Gson user;

    public String getText() {
        return text;
    }
  /**
     * This method is used to get correct date format 
     * @return String is date for this twitter
     * @throws Exception
     */
     
    public String getDate() throws Exception {

        String[] dates = created_at.split(" ");
        StringBuilder date = new StringBuilder();
        date.append(dates[5]);
        date.append("-");
        Calendar cal = Calendar.getInstance();
        cal.setTime(new SimpleDateFormat("MMM").parse(dates[1]));
        SimpleDateFormat format1 = new SimpleDateFormat("MM");
        String formatted = format1.format(cal.getTime());
        //int monthInt = cal.get(Calendar.MONTH) + 1;
        //date.append(monthInt);
        date.append(formatted);
        date.append("-");
        date.append(dates[2]);
        date.append(" ");
        date.append(dates[3].replaceAll(":", "-"));

        return date.toString();
    }

    public String getID() {
        return id_str;
    }


}
