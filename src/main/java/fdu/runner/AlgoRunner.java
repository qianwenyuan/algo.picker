package fdu.runner;

import fdu.algorithms.Algo;
import fdu.algorithms.AlgoFactory;
import fdu.algorithms.Model;
import fdu.algorithms.Params;
import fdu.input.DataSet;

/**
 * Created by sladezhang on 2016/10/7 0007.
 */
public class AlgoRunner {
    private static final String basePackage = "fdu.algorithms";

    public Model run(String confData) throws Exception {

        AlgoFactory factory = (AlgoFactory) Class.forName(getCompleteFactoryName(confData)).newInstance();

        Algo algo = factory.getAlgo();
        DataSet dataset = factory.getDataSet(confData);
        Params params = factory.getParams(confData);

        return algo.train(dataset, params);
    }

    /**
     * Given a configuration, extract the corresponding name of factory class.
     * Let the algo type be AlgoType and name be Algoname, the corresponding
     * factory path is of the form:
     *
     * {basePackage}.String.toLowercase({AlgoType}).String.toLowercase({Algoname}).{Algoname}Factory
     *
     * @param confData
     * @return factory name with full package path.
     */
    private String getCompleteFactoryName(String confData) {
        String[] ss = confData.split(" ");
        String algoType = ss[0];
        String algoName = ss[1];
        StringBuilder sb = new StringBuilder(basePackage);
        sb.append(".").append(algoType.toLowerCase()).append(".").append(algoName.toLowerCase()).append(".").append(algoName).append("Factory");
        return sb.toString();
    }
}
