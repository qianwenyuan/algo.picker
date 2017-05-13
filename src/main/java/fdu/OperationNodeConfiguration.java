package fdu;

import fdu.service.operation.operators.*;

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
        put("kmeans-model", KMeansModel.class);
        // Newly added
        put("lda-model", LDAModel.class);
        put("randomforest-model", RandomForestModel.class);
        put("randomforest-predict", RandomForestPredict.class);
        put("word2vec", Word2Vec.class);
    }};

}
