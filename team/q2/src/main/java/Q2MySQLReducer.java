/**
 * Created by shenanqi on 3/3/16.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigInteger;

public class Q2MySQLReducer {
    public static void main(String[] args) {
        try {
            //read streaming file
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
            PrintStream out2 = new PrintStream(System.out, true, "UTF-8");
            String word = null;
            String currentWord = null;
            BigInteger count=new BigInteger("0");
            //start to read file
            String input ;
            while ((input= br.readLine() )!= null) {
                String[] parts = input.split("\t");
                word = parts[2];
                //get segmented elements
                try {
                    //get the same article
                    if (currentWord != null && !currentWord.equals(word)) {
                        count.add(new BigInteger("1"));
//                        System.out.println(count.toString()+"\t"+input);
                        out2.println(count.toString()+"\t"+input);
                    }
                    currentWord = word;
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
                // deal with last line
             /*   if (input == null) {
                    if (currentWord != null&&currentCount>100000) {

                    }
                }*/
            }
        } catch (IOException io) {
            io.printStackTrace();
        }
    }
}
