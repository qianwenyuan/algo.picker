package fdu.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by guoli on 2017/5/13.
 */
public class ResultCache {

    //  TODO: add LRU cache to save memory
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
                        callCacheMethod(resultCache.cachedResult);
                    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                        // ignored
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
    }

    private void callCacheMethod(Object o) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method resultClass = o.getClass().getMethod("cache");
        resultClass.invoke(o);
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
