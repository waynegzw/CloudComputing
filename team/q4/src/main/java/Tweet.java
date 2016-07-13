/**
 * Created by xgy on 11/04/16.
 */

import java.util.*;

/**
 * Created by Zhangwei on 4/11/16.
 */
public class Tweet {

    public String tweetid;
    public HashMap<String, String> tweetFields;

    public Tweet(String tweetid) {
        this.tweetFields = new HashMap<>();
        this.tweetid = tweetid;
    }

    private String encode(String origin) {
        if (origin != null && !origin.isEmpty()) {
            return Base64.getEncoder().encodeToString(origin.getBytes());
        }
        return null;
    }

    private String decode(String base64Str) {
        if (base64Str != null && !base64Str.isEmpty()) {
            return new String(Base64.getDecoder().decode(base64Str));
        }
        return null;
    }

    public void putField(String key, String val) {
//        tweetFields.put(key, decode(val));
        if (val == null) {
            tweetFields.put(key, "");
        } else {
            tweetFields.put(key, val);
        }
    }

    public String getField(String key) {
//        return encode(tweetFields.get(key));
        return tweetFields.get(key);
    }

    public List<String> getFieldsName() {
        return new ArrayList<String>(tweetFields.keySet());
    }

    public List<String> getFieldsValue() {
        //the values returned are not in base64
        return new ArrayList<String>(tweetFields.values());
//        List<String> list = new ArrayList<String>();
//        Iterator<String> it = tweetFields.values().iterator();
//        while (it.hasNext()) {
////            list.add(encode(it.next()));
//        }
//        return list;
    }

    public String getUpdateSQL() {
        if (tweetFields.isEmpty()) return null;
        Iterator<Map.Entry<String, String>> it = tweetFields.entrySet().iterator();
        StringBuilder up = new StringBuilder();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            up.append(entry.getKey()).append("=").append("'").append(entry.getValue()).append("', ");
        }
        up.setLength(up.length() - 2);
        return "UPDATE q4table SET " + up.toString() + " WHERE tweetid = " + this.tweetid;
    }

    public String getInsertSQL() {
        if (tweetFields.isEmpty()) return null;
        Iterator<Map.Entry<String, String>> it = tweetFields.entrySet().iterator();
        StringBuilder insert_fields = new StringBuilder("tweetid,");
        StringBuilder values_fields = new StringBuilder(this.tweetid + ", ");
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            insert_fields.append(entry.getKey()).append(",");
            values_fields.append("'").append(entry.getValue()).append("',");
        }
        insert_fields.setLength(insert_fields.length() - 1);
        values_fields.setLength(values_fields.length() - 1);

        return "INSERT INTO q4table (" + insert_fields.toString() + ") VALUES "
                + "(" + values_fields.toString() + ")";
    }
}
