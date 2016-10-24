package fdu.algorithms.clustering.kmedoids;

import fdu.algorithms.Algo;
import fdu.algorithms.Params;
import fdu.algorithms.Vector;
import fdu.input.DataSet;


/**
 * Created by sladezhang on 2016/10/7 0007.
 */
public class KMedoids implements Algo<Integer, Vector> {
    @Override
    public KMedoidsModel train(DataSet<Vector> data, Params param) {
        if (!(param instanceof KMedoidsParams)){
            throw new IllegalArgumentException("Param is not instance of KMedoids Params.");
        }

        /* TODO: logic to fill. */
        System.out.println("Training KMedoids model using data:");
        for (Vector v: data){
            System.out.println(v);
        }
        return null;
    }
}
