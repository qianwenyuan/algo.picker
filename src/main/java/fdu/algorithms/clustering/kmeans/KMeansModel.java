package fdu.algorithms.clustering.kmeans;

import fdu.algorithms.Model;
import fdu.algorithms.PredictResult;
import fdu.algorithms.Vector;

import java.util.ArrayList;

/**
 * Created by sladezhang on 2016/10/7 0007.
 */
public class KMeansModel implements Model<Integer,Vector> {
    private ArrayList<Vector> centers;

    public KMeansModel(ArrayList<Vector> centers) {
        this.centers = new ArrayList<>(centers);
    }

    public int size(){
        return centers.size();
    }

    /**
     *
     * @param elem
     * @return cluster index this elem belongs to
     */
    @Override
    public PredictResult<Integer> predict(Vector elem) {
        return new PredictResult<>(0);
    }
}
