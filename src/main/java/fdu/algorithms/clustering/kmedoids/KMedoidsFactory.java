package fdu.algorithms.clustering.kmedoids;

import fdu.algorithms.Algo;
import fdu.algorithms.AlgoFactory;
import fdu.algorithms.Params;
import fdu.algorithms.Vector;
import fdu.input.DataSet;
import fdu.input.FileBasedDataSet;
import fdu.utils.String2VectorConverter;

import java.io.File;

/**
 * Created by sladezhang on 2016/10/7 0007.
 */
public class KMedoidsFactory implements AlgoFactory{
    @Override
    public Algo getAlgo() {
        return new KMedoids();
    }

    @Override
    public DataSet getDataSet(String confData) {
        return new FileBasedDataSet<Vector>(new File(getFileName(confData)), new String2VectorConverter());
    }

    @Override
    public Params getParams(String confData) {
        return new KMedoidsParams(getK(confData));
    }

    private String getFileName(String confData) {
        return confData.split(" ")[3];
    }

    private int getK(String confData) {
        return Integer.parseInt(confData.split(" ")[2]);
    }
}
