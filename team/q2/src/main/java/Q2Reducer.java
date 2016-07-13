/**
 * Created by shenanqi on 3/3/16.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;

public class Q2Reducer {
    public static void main(String[] args) {
        try {
            //read streaming file
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
            PrintStream out2 = new PrintStream(System.out, true, "UTF-8");
            String word = null;
            String currentWord = null;

            //start to read file
            String input;
            while ((input = br.readLine()) != null) {

                try {
                    //just print the input without further process
                    out2.println(input);
//                    }
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException io) {
            io.printStackTrace();
        }
    }
}
