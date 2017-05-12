package fdu.service.operation;

import org.apache.spark.ml.Model;
import org.apache.spark.sql.SparkSession;

/**
 * Created by guoli on 2017/5/6.
 */
public interface CanProduceModel {

    Model execute(SparkSession spark);

    String modelType();

}