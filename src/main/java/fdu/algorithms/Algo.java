package fdu.algorithms;

import fdu.algorithms.Model;
import fdu.algorithms.Params;
import fdu.input.DataSet;

/**
 * Created by sladezhang on 2016/10/1 0001.
 */
public interface Algo<R, E>{
    Model<R, E> train(DataSet<E> data, Params param);
}
