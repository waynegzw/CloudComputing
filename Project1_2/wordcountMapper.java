import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
 
public class wordcountMapper{
 
	public static void main (String args[]) {
 
	try{
		BufferedReader br = 
                      new BufferedReader(new InputStreamReader(System.in));
        String input;
        //While we have input on stdin
		while((input=br.readLine())!=null){
		    //Initialize Tokenizer on string input
            StringTokenizer tokenizer = new StringTokenizer(input);
            while(tokenizer.hasMoreTokens())
            {
                String word = tokenizer.nextToken();  //Get the next word
                System.out.println(word+"\t"+"1");    //Output word\t1
            }
		}
 
	}catch(IOException io){
		io.printStackTrace();
	}	
  }
}