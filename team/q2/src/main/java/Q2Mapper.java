import com.google.gson.*;
import org.apache.commons.lang3.StringEscapeUtils;
import org.codehaus.janino.UnicodeUnescapeReader;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is mapper class
 *
 * @author shenanqi
 * @version 1.0
 * @since 01/29/2016
 */
public class Q2Mapper {


    public static HashSet<String> stopset = new HashSet<>();
    public static HashMap<String, Integer> score = new HashMap<>();
    public static HashSet<String> banned = new HashSet<>();

    public static void main(String[] args) {
        BufferedReader br = null;
        try {

            String sCurrentLine;
            br = new BufferedReader(new InputStreamReader(Q2Mapper.class.getResourceAsStream("stopwords.txt")));


            sCurrentLine = br.readLine();
            String[] stopwords = sCurrentLine.split(",");

            for (int i = 0; i < stopwords.length; i++) {
                stopset.add(stopwords[i]);
            }
            br.close();
        } catch (NullPointerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        BufferedReader br2 = null;
        try {
            String sCurrentLine;
            br2 = new BufferedReader(new InputStreamReader(Q2Mapper.class.getResourceAsStream("score.txt")));
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

            br3 = new BufferedReader(new InputStreamReader(Q2Mapper.class.getResourceAsStream("banned.txt")));

            while ((sCurrentLine = br3.readLine()) != null) {
                char[] bans = sCurrentLine.toCharArray();
                for (int i = 0; i < bans.length; i++) {
                    if (bans[i] >= '0' && bans[i] <= '9') {

                    }
                    else if ((int) bans[i] >= 97 && (int) bans[i] <= 109) {
                        bans[i] = (char) ((int) bans[i] + 13);
                    }
                    else {
                        bans[i] = (char) ((int) bans[i] - 13);
                    }
                }
                banned.add(String.valueOf(bans));
            }
            br3.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            String sCurrentLine;
            BufferedReader br4 = new BufferedReader(new InputStreamReader(System.in));
//            BufferedReader br4 = new BufferedReader(new InputStreamReader(Q2Mapper.class.getResourceAsStream("test10.txt")));
            FileOutputStream fos = new FileOutputStream("test2");
            Writer out = new OutputStreamWriter(fos, "UTF8");
            PrintStream out2 = new PrintStream(System.out, true, "UTF-8");

            while ((sCurrentLine = br4.readLine()) != null) {
                ArrayList<StringBuffer> list = filter(sCurrentLine);
                for (int i = 0; i < list.size(); i++) {
//                    out.write(list.get(i).toString());
//                    System.out.println(list.get(i).toString());
                    out2.println(list.get(i).toString());
                }
            }
            out.close();
            br4.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static ArrayList filter(String tweetjson) {

        Gson gson = new Gson();
        JsonParser jsonParser = new JsonParser();

        JsonObject user = jsonParser.parse(tweetjson)
                .getAsJsonObject().getAsJsonObject("user");
        String userid = user.get("id").getAsString();

        JsonObject hashtag = jsonParser.parse(tweetjson)
                .getAsJsonObject().getAsJsonObject("entities");

        JsonArray hashtags = hashtag.get("hashtags").getAsJsonArray();
        String[] tags = new String[hashtags.size()];
        for (int i = 0; i < hashtags.size(); i++) {
            JsonElement onehash = hashtags.get(i);
            JsonObject tep = jsonParser.parse(onehash.toString()).getAsJsonObject();
            String tag = tep.get("text").getAsString();
            tags[i] = tag;
        }

        Twitter t = gson.fromJson(tweetjson, Twitter.class);

        String original = t.getText();
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
        String finalscore = (effectiveWord == 0) ? "0.000" : (String.format("%.3f", (double) sentscore / effectiveWord));

        int start = 0;
        int end = 0;
        char[] input = original.toCharArray();
        StringBuilder result = new StringBuilder();
        StringBuilder temp = new StringBuilder();
        for (int i = 0; i < input.length; i++) {
            end = i;
            if ((input[i] >= '0' && input[i] <= '9') || (input[i] >= 'a' && input[i] <= 'z')) {
                temp.append(input[i]);
            }
            else if (input[i] >= 'A' && input[i] <= 'Z') {
                temp.append((char) (input[i] - 'A' + 'a'));
            }
            // here splitter
            else {
                if (banned.contains(temp.toString())) {
                    //jia ***
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
            //  System.out.println(temp.toString());
            result.append(input[start]);

            for (int j = 0; j < temp.length() - 2; j++) {
                result.append("*");
            }
            if (end == input.length - 1) {
                result.append(input[end]);

            }
            else {
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

            ArrayList<StringBuffer> resultlist = new ArrayList<>();

            for (int i = 0; i < tags.length; i++) {
                StringBuilder finalresult = new StringBuilder();
                finalresult.append(userid).append("_");
                finalresult.append(tags[i]).append("_");
                finalresult.append(t.getID()).append("\t");

                finalresult.append(finalscore).append(":");
                finalresult.append(t.getDate()).append(":");
                finalresult.append(StringEscapeUtils.escapeJava(result.toString()));

                String str = finalresult.toString();
                StringReader sr = new StringReader(str);
                UnicodeUnescapeReader uur = new UnicodeUnescapeReader(sr);

                StringBuffer buf = new StringBuffer();
                for (int c = uur.read(); c != -1; c = uur.read()) {
                    buf.append((char) c);
                }
                resultlist.add(buf);
            }
            return resultlist;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}
