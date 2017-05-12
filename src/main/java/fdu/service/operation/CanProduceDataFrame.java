package fdu.service.operation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by guoli on 2017/5/11.
 */
public interface CanProduceDataFrame {
    Dataset<Row> execute(SparkSession spark);
}
