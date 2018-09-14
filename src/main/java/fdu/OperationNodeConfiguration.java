package fdu;

import fdu.service.operation.operators.*;
import javassist.runtime.Desc;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by slade on 2016/11/23.
 */
public class OperationNodeConfiguration {
    /*
       Node name configuration.
     */
    public static final String DATASOURCE = "data-source";
    public static final String JOIN = "sql-join";
    public static final String PROJECT = "project";
    public static final String FILTER = "filter";
    public static final String KMEANS_MODEL = "kmeans-model";
    // public static final String KMEANS = "kmeans";

    public static final Map<String, Class> opMap = new HashMap<String, Class>() {{
        put("data-source", DataSource.class);
        put("sql-join", Join.class);
        put("project", Project.class);
        put("filter", Filter.class);
        // Newly added
        put("sample", Sample.class); //
        put("lda-model", LDAModel.class); //
        put("word2vec", Word2Vec.class); //
        put("onehot", OneHotEncoder.class);
        put("vector-asm", VectorAssembler.class); //

        // Regression
        put("linear-regression-model", LinearRegressionModel.class);
        put("linear-regression-predict", LinearRegressionPredict.class);
        put("logistic-regression-model", LogisticRegressionModel.class);
        put("logistic-regression-predict", LogisticRegressionPredict.class);


        //aggregation
        put("groupby-count", GroupBy_Count.class);
        put("groupby-max", GroupBy_Max.class);
        put("groupby-sum", GroupBy_Sum.class);
        put("max", Max.class);
        put("min", Min.class);
        put("sum", Sum.class);
        put("column", Column.class);
        put("count", Count.class);
        put("count-distinct", Count_distinct.class);
        put("avg", Avg.class);
        put("topn", TopN.class);
        put("collect_set", Collect_set.class);
        put("collect_list", Collect_list.class);
        put("corr", Corr.class);
        put("covar-pop", Covar_pop.class);
        put("covar-samp", Covar_sample.class);
        put("var-pop", Var_pop.class);
        put("var-samp", Var_sample.class);
        put("stddev-pop", Stddev_pop.class);
        put("stddev-samp", Stddev_sample.class);
        put("kurtosis", Kurtosis.class);
        put("skewness", Skewness.class);
        put("union", Union.class);


        //classification
        put("naivebayes-model",NaiveBayesModel.class);
        put("naivebayes-predict",NaiveBayesPredict.class);
        put("decisiontree-model",DecisionTreeClassificationModel.class);
        put("decisiontree-predict",DecisionTreeClassificationPredict.class);
        put("randomforest-model", RandomForestModel.class);
        put("randomforest-predict", RandomForestPredict.class);

        //clusstering
        put("kmeans-model", KMeansModel.class); //
    }};


}
