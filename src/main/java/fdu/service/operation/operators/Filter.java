/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.service.operation.operators;

import fdu.bean.generator.OperatorVisitor;
import fdu.service.operation.CanProduceDataFrame;
import fdu.service.operation.SqlOperation;
import fdu.service.operation.UnaryOperation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;

/**
 *
 * @author Lu Chang
 */
public class Filter extends UnaryOperation implements SqlOperation {
    private String name;
    private String condition;

    public Filter(String id, String type, String z) {
        super(id, type, z);
    }

    @Override
    public Dataset<Row> execute(SparkSession spark) {
        return ((CanProduceDataFrame)getLeft()).execute(spark).filter(condition);
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        getLeft().accept(visitor);
        visitor.visitFilter(this);
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getName() {
        return name;
    }

    public String getCondition() {
        return condition;
    }

    public static Filter newInstance(JSONObject obj){
        Filter result = new Filter(obj.getString("id"), obj.getString("type"), obj.getString("z"));
        result.setName(obj.getString("name"));
        result.setCondition(obj.getString("condition"));
        return result;
    }

    @Override
    public String toString() {
        return "([Filter name: " + name + " condition: " + condition  + "]" + getLeft() + ")";
    }

    @Override
    public String toSql() throws ClassCastException {
        SqlOperation left = (SqlOperation) getLeft();
        if (left instanceof DataSource)
            return  ((SqlOperation)getLeft()).toSql() + " WHERE " + condition;
        else if (left instanceof Join)
            return ((SqlOperation)getLeft()).toSql() + " WHERE " + condition;
        else throw new UnsupportedOperationException("Invalid filter node");
    }
}
