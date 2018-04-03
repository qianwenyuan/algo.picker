package fdu.util;

import fdu.service.operation.Operation;
import org.apache.spark.ml.util.MLWritable;
import org.apache.spark.sql.Dataset;

import java.io.IOException;
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
            return null;
        } else {
            if (resultCache.cachedResult != null) {
                if (!resultCache.cached) {
                    try {
                        callCacheMethod(o, resultCache.cachedResult);
                        resultCache.cached = true;
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
        cacheMap.put(o, new CacheEntry(result));
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

        CacheEntry(Object cachedResult) {
            this.cachedResult = cachedResult;
        }
    }
}
