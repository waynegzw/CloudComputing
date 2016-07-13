import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Reducer {

    public static void main(String args[]) {

        try {
            BufferedReader br =
            new BufferedReader(new InputStreamReader(System.in));
            //Initialize Variables
            String input;
            String title = null;
            String currentTitle = null;
            int[] currentPageViews = new int[31];
            int date, index, pageViews, offset = 20151201;

            //While we have input on stdin
            while ((input = br.readLine()) != null) {
                try {
                    String[] parts = input.split("\t");
                    title = parts[0];
                    date = Integer.parseInt(parts[1]);
                    //use offset to calculate the index
                    offset = Integer.parseInt(parts[1].substring(0, 6) + "01");
                    index = date - offset;
                    pageViews = Integer.parseInt(parts[2]);

                    //We have sorted input, so check if we
                    //are we on the same title?
                    if (currentTitle != null && currentTitle.equals(title))
                        currentPageViews[index] += pageViews;
                    else //The title has changed
                    {
                        if (currentTitle != null) { //Is this the first title, if not output count                           
                            int total = 0;
                            for (int i = 0; i < 31; i++) {
                                // calculate page views for the whole month
                                total += currentPageViews[i];
                            }
                            if (total > 100000){
                                String value = "";
                                for (int i = 0; i < 31; i++) {
                                    // concat daily page views into one string
                                    value += "\t" + (i + offset) + ":" + currentPageViews[i];
                                }
                                System.out.println(total + "\t" + currentTitle + value);
                            }
                        }
                        currentTitle = title;
                        currentPageViews = new int[31];
                        currentPageViews[index] = pageViews;
                    }
                } catch (NumberFormatException e) {
                    continue;
                }
            }

            //Print out last word if missed
            if (currentTitle != null && currentTitle.equals(title)) {
                int total = 0;
                for (int i = 0; i < 31; i++) {
                    total += currentPageViews[i];
                }
                if (total > 100000){
                    String value = "";
                    for (int i = 0; i < 31; i++) {
                        // concat daily page views into one string
                        value += "\t" + (i + offset) + ":" + currentPageViews[i];
                    }
                    System.out.println(total + "\t" + currentTitle + value);
                }
            }

        } catch (IOException io) {
            io.printStackTrace();
        }
    }
}