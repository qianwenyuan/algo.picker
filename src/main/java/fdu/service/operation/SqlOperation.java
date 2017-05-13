package fdu.service.operation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


/**
 * Created by guoli on 2017/5/2.
 */
public interface SqlOperation extends CanProduce<Dataset<Row>> {

    String toSql() throws ClassCastException;

}
