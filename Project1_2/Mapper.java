import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

public class Mapper {

    public static void main(String args[]) {

        try {
            BufferedReader br =
            new BufferedReader(new InputStreamReader(System.in));
            String line;
            String date = "";
            //While we have input on stdin
            while ((line = br.readLine()) != null) {
                String input[] = line.split(" ");
                // filter the data
                if (input.length == 4 && !input[1].equals("") && input[0].equals("en") &&
                    !input[1].matches("^[a-z].*") &&
                    !input[1].matches("^(Media|Special|Talk|User|User_talk|Project|Priject_talk|File|File_talk|" +
                        "MediaWiki|MediaWiki_talk|Template|Template_talk|Help|Help_talk|Category|Category_talk|" +
                        "Portal|Wikipedia|Wikipedia_talk):.*") &&
                    !input[1].matches("^.*\\.(jpg|gif|png|JPG|GIF|PNG|txt|ico)$") &&
                    !input[1].matches("^(404_error/|Main_Page|Hypertext_Transfer_Protocol|Search)$")) {

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