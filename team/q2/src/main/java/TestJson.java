
import com.google.gson.*;
import org.apache.commons.lang3.StringEscapeUtils;
import org.codehaus.janino.UnicodeUnescapeReader;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by shenanqi on 2/28/16.
 * This class is used for data cleaning.
 */
public class TestJson {
    // to store stop words
    public static HashSet<String> stopset = new HashSet<>();
    // to store words and scores
    public static HashMap<String, Integer> score = new HashMap<>();
    // to store banned words
    public static HashSet<String> banned = new HashSet<>();

    public static void main(String args[]) throws IOException {

        /* Stop words */
        BufferedReader br = null;
        try {

            String sCurrentLine;
            br = new BufferedReader(new InputStreamReader(TestJson.class.getResourceAsStream("stopwords.txt")));
                    sCurrentLine = br.readLine();
                    // store all stopword into this array
            String[] stopwords = sCurrentLine.split(",");
            // and stored into a HashSet
            for (int i = 0; i < stopwords.length; i++) {
                stopset.add(stopwords[i]);
            }
            br.close();
        } catch (NullPointerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

         /* Words score */
        BufferedReader br2 = null;
        try {
            String sCurrentLine;
            br2 = new BufferedReader(new InputStreamReader(TestJson.class.getResourceAsStream("score.txt")));
            // put all word and corresponding score into score hashmap
            while ((sCurrentLine = br2.readLine()) != null) {
                score.put(sCurrentLine.split("\t")[0], Integer.parseInt(sCurrentLine.split("\t")[1]));
            }
            br2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        /* Scan banned word */
        BufferedReader br3 = null;
        try {
            String sCurrentLine;

        br3 = new BufferedReader(new InputStreamReader(TestJson.class.getResourceAsStream("banned.txt")));
            // decipher all banned words and stored in banned HashSet
            while ((sCurrentLine = br3.readLine()) != null) {
                char[] bans = sCurrentLine.toCharArray();
                for (int i = 0; i < bans.length; i++) {
                    if(bans[i]>='0'&&bans[i]<='9'){

                    }else if ((int) bans[i] >= 97 && (int) bans[i] <= 109) {
                        bans[i] = (char) ((int) bans[i] + 13);
                    }
                    else {
                        bans[i] = (char) ((int) bans[i] - 13);
                    }
                }
               // System.out.println(String.valueOf(bans));
                banned.add(String.valueOf(bans));
            }
            br3.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        /* Read and clean test data */
        BufferedReader br4 = null;
        try {
            String sCurrentLine;

            PrintWriter writer = new PrintWriter("output-test", "UTF-8");
            br4 = new BufferedReader(new FileReader("/home/ubuntu/part-00000"));
         
            while ((sCurrentLine = br4.readLine()) != null) {
                writer.println(filter(sCurrentLine));
          
            }
            br4.close();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
/**
 * This method is used to get clean result
  * @param tweetjson twitter json
  * @return String clean string
 */
 
    public static String filter(String tweetjson) {

        Gson gson = new Gson();
        JsonParser jsonParser = new JsonParser();

        JsonObject user = jsonParser.parse(tweetjson)
                .getAsJsonObject().getAsJsonObject("user");
        String userid = user.get("id").getAsString();

        JsonObject hashtag = jsonParser.parse(tweetjson)
                .getAsJsonObject().getAsJsonObject("entities");

        JsonArray hashtags = hashtag.get("hashtags").getAsJsonArray();
        // get all hashtag for this twitter
        String[] tags = new String[hashtags.size()];
        for (int i = 0; i < hashtags.size(); i++) {
            JsonElement onehash = hashtags.get(i);
            JsonObject tep = jsonParser.parse(onehash.toString()).getAsJsonObject();
            String tag = tep.get("text").getAsString();
            tags[i] = tag;
        }
        // create twitter object using gson and get useful information
        Twitter t = gson.fromJson(tweetjson, Twitter.class);

        String original = t.getText();
        //convert into string array
        String[] parts = t.getText().split("[^a-zA-Z0-9]");
    
        for (int i = 0; i < parts.length; i++) {
            if (!parts[i].equals("")) {
                if (stopset.contains(parts[i].toLowerCase())) {
                    parts[i] = "";
                }
            }
        }

        /* Calculate score */
        int sentscore = 0;
        int effectiveWord = 0;
        for (int i = 0; i < parts.length; i++) {
            if (!parts[i].equals("")) {
                effectiveWord++;
                if (score.containsKey(parts[i].toLowerCase())) {

                    sentscore = sentscore + score.get(parts[i].toLowerCase());
                }

            }
        }
        // get final score
        String finalscore = (effectiveWord == 0) ? "0.000" : (String.format("%.3f", (double) sentscore / effectiveWord));

        int start = 0;
        int end = 0;
        char[] input = original.toCharArray();
        StringBuilder result = new StringBuilder();
        StringBuilder temp = new StringBuilder();
        for (int i = 0; i < input.length; i++) {
            end = i;
            // remove case sensitve
            if ((input[i] >= '0' && input[i] <= '9') || (input[i] >= 'a' && input[i] <= 'z')) {
                temp.append(input[i]);
            }
            else if (input[i] >= 'A' && input[i] <= 'Z') {
                temp.append((char) (input[i] - 'A' + 'a'));
            }
             // convert letters to * for banned words
            else {
                if (banned.contains(temp.toString())) {
                    result.append(input[start]);
                    for (int j = 0; j < temp.length() - 2; j++) {
                        result.append("*");
                    }
                    result.append(input[end - 1]);
                    result.append(input[end]);
                    start = end + 1;
                }
                else {
                    for (int j = start; j <= end; j++) {
                        result.append(input[j]);
                    }
                    start = end + 1;
                }
                temp = new StringBuilder();
            }
        }
        
        if (banned.contains(temp.toString())) {
            result.append(input[start]);
            for (int j = 0; j < temp.length() - 2; j++) {
                result.append("*");
            }
            if(end==input.length-1){
                result.append(input[end]);

            }else{
                result.append(input[end - 1]);
                result.append(input[end]);
            }
        }
        else {
            for (int j = start; j <= end; j++) {
                result.append(input[j]);
            }
        }

        try {
            //get final result by joining parts together
            StringBuilder finalresult = new StringBuilder();
            finalresult.append(t.getID()).append("\t");
            finalresult.append(userid).append("\t");
            finalresult.append(t.getDate()).append("\t");
            finalresult.append(finalscore).append("\t");
            // to keep all escape character in java
            finalresult.append(StringEscapeUtils.escapeJava(result.toString()));

            for (int i = 0; i < tags.length; i++) {
                finalresult.append("\t").append(tags[i]);
            }
            String str =finalresult.toString() ;
            StringReader sr = new StringReader(str);
            // convert unicode to readable character
            UnicodeUnescapeReader uur = new UnicodeUnescapeReader(sr);
            StringBuffer buf = new StringBuffer();
            for(int c = uur.read(); c != -1; c = uur.read())
            {
                buf.append((char)c);
            }
            return buf.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }


}
