package fdu.util;

import fdu.service.operation.Operation;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.util.MLWritable;
import org.apache.spark.sql.Dataset;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by guoli on 2017/5/13.
 */
public class ResultCache {

    //  TODO: Rewrite as LRU cache to save memory
    private final Map<Object, CacheEntry> cacheMap = new HashMap<>();

    public <T> T query(Object o) {
        CacheEntry resultCache = cacheMap.get(o);
        if (resultCache == null) {
            cacheMap.put(o, new CacheEntry());
        } else {
            if (resultCache.cachedResult != null) {
                if (!resultCache.cached) {
                    try {
                        resultCache.cached = true;
                        callCacheMethod(o, resultCache.cachedResult);
                    } catch (IOException e) {
                        e.printStackTrace(); // ignored
                    }
                }
                resultCache.usedTimes++;
                return (T) (resultCache.cachedResult); // unchecked ignored
            }
        }
        return null;
    }

    public <T> void commit(Object o, T result) {
        if (cacheMap.containsKey(o)) {
            // re-commit
            cacheMap.put(o, new CacheEntry(result));
        } else {
            cacheMap.get(o).cachedResult = result;
        }
        if (o instanceof Operation && ((Operation) o).isNeedCache()) {
            try {
                callCacheMethod(o, cacheMap.get(o));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void callCacheMethod(Object node, Object o) throws IOException {
        Operation op = (Operation) node;
        if (o instanceof Dataset) {
            ((Dataset) o).cache();
        } else if (o instanceof MLWritable) {
            ((MLWritable) o).save(op.getName());
        }
    }

    private class CacheEntry {
        int usedTimes = 1;
        boolean cached;
        Object cachedResult;

        CacheEntry() {}

        CacheEntry(Object cachedResult) {
            this.cachedResult = cachedResult;
        }
    }
}
