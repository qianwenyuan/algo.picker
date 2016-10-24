package fdu.algorithms.clustering.kmeans;

import fdu.algorithms.Params;

/**
 * Created by sladezhang on 2016/10/7 0007.
 */
public class KMeansParams implements Params {
    private int k;

    public KMeansParams(int k) {
        this.k = k;
    }

    public int getK() {
        return k;
    }

}
