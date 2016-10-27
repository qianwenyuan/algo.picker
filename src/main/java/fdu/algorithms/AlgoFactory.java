package fdu.algorithms;

import fdu.input.DataSet;
import fdu.operation.OperationTree;

/**
 * Created by sladezhang on 2016/10/7 0007.
 */
public interface AlgoFactory {
    Algo getAlgo();
    DataSet getDataSet(String confData);
    Params getParams(String confData);
    OperationTree getOperationTree();
}
