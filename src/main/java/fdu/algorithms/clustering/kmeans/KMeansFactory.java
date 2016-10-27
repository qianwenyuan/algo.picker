package fdu.algorithms.clustering.kmeans;

import fdu.algorithms.Algo;
import fdu.algorithms.AlgoFactory;
import fdu.algorithms.Params;
import fdu.algorithms.Vector;
import fdu.input.DataSet;
import fdu.input.FileBasedDataSet;
import fdu.operation.OperationTree;
import fdu.utils.String2VectorConverter;

import java.io.File;

/**
 * Created by sladezhang on 2016/10/7 0007.
 */
public class KMeansFactory implements AlgoFactory {
    @Override
    public Algo getAlgo() {
        return new KMeans();
    }

    @Override
    public DataSet getDataSet(String confData) {
        return new FileBasedDataSet<Vector>(new File(getFileName(confData)), new String2VectorConverter());
    }

    private String getFileName(String confData) {
        return confData.split(" ")[3];
    }

    @Override
    public Params getParams(String confData) {
        return new KMeansParams(getK(confData));
    }

    private int getK(String confData) {
        return Integer.parseInt(confData.split(" ")[2]);
    }

    @Override
    public OperationTree getOperationTree() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
