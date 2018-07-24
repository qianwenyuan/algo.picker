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
        put("linear-model", LinearRegressionModel.class);
        put("linear-predict", LinearRegressionPredict.class);
        put("lr-model", LogisticRegressionModel.class);
        put("lr-predict", LogisticRegressionPredict.class);

        //sql
        put("groupby", GroupBy.class);
        put("max", Max.class);
        put("min", Min.class);
        put("sum", Sum.class);
        put("count", Count.class);
        put("avg", Avg.class);
        put("topn", TopN.class);
        put("asc", Asc.class);
        put("desc", Desc.class);
        put("corr", Corr.class);
        put("column", Column.class);
        put("sin", Sin.class);
        put("asin", Asin.class);
        put("cos", Cos.class);
        put("acos", Acos.class);
        put("tan", Tan.class);
        put("atan", Atan.class);

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
