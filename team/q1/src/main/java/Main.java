import java.io.*;
import io.undertow.*;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Created by xgy on 27/02/16.
 */
public class Main {
    public static void main(String[] args) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        System.out.println(ZonedDateTime.now().format(formatter));
//        String e = "aahhff\\\\fs\\fs";
//        System.out.println(e);
//        String b = e.replace("\\\\", "\\\\\\");
//        System.out.println(b);
        BufferedReader br ;
        try {
            br = new BufferedReader(new FileReader("/home/xgy/test.txt"));
            String cur = br.readLine();
            String n = cur.replaceAll("\\\\", "\\\\");
            System.out.print(n);

        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        } catch (IOException e2) {
            e2.printStackTrace();
        }
    }
}
