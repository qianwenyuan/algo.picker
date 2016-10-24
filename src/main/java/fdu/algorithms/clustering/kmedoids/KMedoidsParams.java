package fdu.algorithms.clustering.kmedoids;

import fdu.algorithms.Params;

/**
 * Created by sladezhang on 2016/10/7 0007.
 */
public class KMedoidsParams implements Params {
    private int k;

    public KMedoidsParams(int k) {
        this.k = k;
    }

    public int getK() {
        return k;
    }

}
