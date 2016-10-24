package fdu.algorithms.clustering.kmeans;

import fdu.algorithms.Algo;
import fdu.algorithms.Params;
import fdu.algorithms.Vector;
import fdu.input.DataSet;

/**
 * Created by sladezhang on 2016/10/3 0003.
 */
public class KMeans implements Algo<Integer, Vector> {
    @Override
    public  KMeansModel train(DataSet<Vector> data, Params param) {
        if ((!(param instanceof KMeansParams))) {
            throw new IllegalArgumentException("Param should be the instance of KMeansParams.");
        }

        /* TODO: logic to fill. */
        System.out.println("Training KMeans model using data:");
        for (Vector v: data){
            System.out.println(v);
        }
        return null;
    }
}
