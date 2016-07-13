import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Zhangwei on 3/22/16.
 */
public class Q3WorldCount {
//    public static HashSet<String> stopset = new HashSet<>();

//    static {
//    /* Stop words */
//        BufferedReader br = null;
//        try {
//            String sCurrentLine;
//            br = new BufferedReader(new InputStreamReader(Q3WorldCount.class.getResourceAsStream("stopwords.txt")));
//            sCurrentLine = br.readLine();
//            // store all stopword into this array
//            String[] stopwords = sCurrentLine.split(",");
//            // and stored into a HashSet
//            for (int i = 0; i < stopwords.length; i++) {
//                stopset.add(stopwords[i]);
//            }
//            br.close();
//        } catch (NullPointerException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    public static String worldCount(String text, String search) {
        String[] searchWords = search.split(",");
        String[] words = text.replaceAll("(https?|ftp)://[^\\s/$.?#][^\\s]*", "").split("[^a-zA-Z0-9]");
        Stream<String> stream = Arrays.stream(words);

        //http://ec2-54-84-141-209.compute-1.amazonaws.com/q3?start_date=2014-04-01&end_date=2014-05-28&start_userid=51538630&end_userid=51539182&words=u,petition,loving
        //http://q1-1848733628.us-east-1.elb.amazonaws.com/q3?start_date=2014-04-01&end_date=2014-05-28&start_userid=51538630&end_userid=51539182&words=u,petition,loving
        Map<String, Integer> phrase = stream.map(String::toLowerCase).filter(c -> Arrays.asList(searchWords).contains(c))
                .collect(Collectors.groupingBy(String::toString, Collectors.summingInt(s -> 1)));
        if (!phrase.containsKey(searchWords[0])) {
            phrase.put(searchWords[0], 0);
        }
        if (!phrase.containsKey(searchWords[1])) {
            phrase.put(searchWords[1], 0);
        }
        if (!phrase.containsKey(searchWords[2])) {
            phrase.put(searchWords[2], 0);
        }
        return searchWords[0] + ":" + phrase.get(searchWords[0]) + "\n" +
                searchWords[1] + ":" + phrase.get(searchWords[1]) + "\n" +
                searchWords[2] + ":" + phrase.get(searchWords[2]) + "\n";

    }

    public static void main(String[] args) {
        System.out.println(worldCount("one Fish RED two fish red fish blue fish", "tt,fish,one"));
        //System.out.println(worldCount("this is the my first twwetts, this is really from : http://t.co/iZbYagMN6e hello this.", "this,is,hello"));
    }
}
