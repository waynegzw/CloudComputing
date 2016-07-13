import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
 
public class wordcountReducer{
 
	public static void main (String args[]) {
 
	try{
		BufferedReader br = 
                      new BufferedReader(new InputStreamReader(System.in));
        //Initialize Variables
        String input;
        String word = null;
        String currentWord = null;
        int currentCount = 0;
        
        //While we have input on stdin
		while((input=br.readLine())!=null){
		    try{
                String[] parts = input.split("\t");
                word = parts[0];
                int count = Integer.parseInt(parts[1]);
                
                //We have sorted input, so check if we
                //are we on the same word?
                if(currentWord!=null && currentWord.equals(word))
                    currentCount++;
                else //The word has changed
                {
                    if(currentWord!=null) //Is this the first word, if not output count
                        System.out.println(currentWord+"\t"+currentCount);
                    currentWord=word;
                    currentCount=count;
                }
            }
            catch(NumberFormatException e){
                continue;
            }
		}
        
        //Print out last word if missed
        if(currentWord!=null && currentWord.equals(word))
        {
            System.out.println(currentWord+"\t"+currentCount);
        }
 
	}catch(IOException io){
		io.printStackTrace();
	}	
  }
}