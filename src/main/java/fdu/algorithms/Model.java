package fdu.algorithms;

/**
 * Created by sladezhang on 2016/10/7 0007.
 */
public interface Model<R,E> {
    PredictResult<R> predict(E elem);
}
