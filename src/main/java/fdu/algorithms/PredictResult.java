package fdu.algorithms;

/**
 * Created by sladezhang on 2016/10/7 0007.
 */
public class PredictResult<T> {
    private T result;

    public PredictResult(T result){
        this.result = result;
    }

    public T getResult() {
        return result;
    }
}
