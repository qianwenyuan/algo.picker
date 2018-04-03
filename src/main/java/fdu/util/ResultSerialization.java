package fdu.util;

import org.apache.spark.ml.Model;
import org.apache.spark.sql.Dataset;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;

/**
 * Created by liangchg on 2017/5/17.
 */
public class ResultSerialization {

    public static final int MAX_RESULT_OUTPUT = 500;

    public static String toString(Object result) {
        JSONObject o = new JSONObject();
        if (result instanceof Dataset<?>) {
            Dataset<?> set = (Dataset<?>) result;
            o.put("type", "table");
            // List<String> array = set.toJSON().takeAsList(300);
            List<String> array = set.toJSON().takeAsList(MAX_RESULT_OUTPUT);
            o.put("data", new JSONArray(array));
        } else if (result instanceof Model<?>) {
            o.put("type", "model");
            o.put("data", result.toString());
        }
        return o.toString();
    }

}
