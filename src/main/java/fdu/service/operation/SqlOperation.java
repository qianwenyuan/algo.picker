package fdu.service.operation;

/**
 * Created by guoli on 2017/5/2.
 */
public interface SqlOperation extends CanProduceDataFrame {

    String toSql() throws ClassCastException;

}
