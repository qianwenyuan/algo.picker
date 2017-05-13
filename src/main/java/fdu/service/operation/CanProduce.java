package fdu.service.operation;

import fdu.util.ResultCache;
import fdu.util.UserSession;

/**
 * Created by guoli on 2017/5/11.
 */
public interface CanProduce<T> {

    T execute(UserSession user);

    default T executeCached(UserSession user) {
        ResultCache cache = user.getResultCache();
        T result = cache.query(this);
        if (result != null) {
            return result;
        } else {
            result = execute(user);
            cache.commit(this, result);
        }
        return result;
    }

}
