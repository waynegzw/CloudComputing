import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Mapper {

    public static void main(String args[]) {

        try {
            BufferedReader br =
                    new BufferedReader(new InputStreamReader(System.in));
            String line;
            String date = "";
            Pattern p1 = Pattern.compile("^[a-z].*");
            Pattern p2 = Pattern.compile("^(Media|Special|Talk|User|User_talk|Project|Priject_talk|File|File_talk|" +
                        "MediaWiki|MediaWiki_talk|Template|Template_talk|Help|Help_talk|Category|Category_talk|" +
                        "Portal|Wikipedia|Wikipedia_talk):.*");
            Pattern p3 = Pattern.compile("^.*\\.(jpg|gif|png|JPG|GIF|PNG|txt|ico)$");
            Pattern p4 = Pattern.compile("^(404_error/|Main_Page|Hypertext_Transfer_Protocol|Search)$");
            //While we have input on stdin
            while ((line = br.readLine()) != null) {
                String input[] = line.split(" ");
                // filter the data
                Matcher m1 = p1.matcher(input[1]);
                Matcher m2 = p2.matcher(input[1]);
                Matcher m3 = p3.matcher(input[1]);
                Matcher m4 = p4.matcher(input[1]);

                if (input.length == 4 && !input[1].equals("") && input[0].equals("en") &&
                        !m1.matches() && !m2.matches() && !m3.matches() && !m4.matches()) {

                    // get the map reduce input file name
                    String name = System.getenv("mapreduce_map_input_file");
                    //get the date
                    if (name != null) {
                       date = name.substring(name.length() - 18, name.length() - 10);
                    }

                    // output to stdout
                    System.out.println(input[1] + "\t" + date + "\t" + input[2]);
                }
            }

        } catch (IOException io) {
            io.printStackTrace();
        }
    }
}