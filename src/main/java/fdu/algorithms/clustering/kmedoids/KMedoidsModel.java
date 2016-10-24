package fdu.algorithms.clustering.kmedoids;

import fdu.algorithms.Model;
import fdu.algorithms.PredictResult;
import fdu.algorithms.Vector;

import java.util.ArrayList;

/**
 * Created by sladezhang on 2016/10/7 0007.
 */
public class KMedoidsModel implements Model<Integer, Vector> {
    private ArrayList<Vector> centers;

    public KMedoidsModel(ArrayList<Vector> centers) {
        this.centers = new ArrayList<>(centers);
    }

    public int size(){
        return  centers.size();
    }

    @Override
    public PredictResult<Integer> predict(Vector elem) {
        return new PredictResult<>(0);
    }
}
