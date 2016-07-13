package com.cloudcomputing.samza.pitt_cabs;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;

/**
 * Created by Zhangwei on 4/19/16.
 */
public class JsonTest {
    public static void main(String[] args) {
        HashMap<String, String> h = new HashMap<String, String>();
        h.put("a", "1");
        h.put("b", "2");

        System.out.println(h.entrySet().toString());
        JSONObject result = new JSONObject();
        result.put("name", "my_name");
        result.put("profile", "profile_image_url");
        System.out.println(result.toString());

        JSONArray followers = new JSONArray();
        JSONObject follower = new JSONObject();

        follower.put("name", "follower_name_1");
        follower.put("profile", "profile_image_url");
        followers.put(follower);
        result.put("followers", followers);
        System.out.println(result.toString());

        String s1 = "{\"blockId\":76,\"driverId\":9394,\"latitude\":3075,\"longitude\":3828,\"type\":\"DRIVER_LOCATION\"}";
        String s2 = "{\"blockId\":76,\"driverId\":6977,\"latitude\":3476,\"type\":\"ENTERING_BLOCK\",\"status\":\"AVAILABLE\",\"longitude\":3827,\"gender\":\"M\",\"rating\":4.0,\"salary\":50}";
        System.out.println(s2);

        JSONObject obj = new JSONObject(s2);
        System.out.println(obj.get("blockId"));
    }
}
