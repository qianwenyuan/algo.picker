/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.service.operation.operators;

import fdu.bean.generator.OperatorVisitor;
import fdu.service.operation.BinaryOperation;
import fdu.service.operation.CanProduceDataFrame;
import fdu.service.operation.SqlOperation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions$;
import org.json.JSONObject;

/**
 *
 * @author Lu Chang
 */
public class Join extends BinaryOperation implements SqlOperation {
    private String name;
    private String condition;

    public Join(String id, String type, String z) {
        super(id, type, z);
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        getLeft().accept(visitor);
        getRight().accept(visitor);
        visitor.visitJoin(this);
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

    public static Join newInstance(JSONObject obj){
        Join result = new Join(obj.getString("id"), obj.getString("type"), obj.getString("z"));
        result.setName(obj.getString("name"));
        result.setCondition(obj.getString("condition"));
        return result;
    }

    @Override
    public String toString() {
        return "([Join : "+ condition +"] " + getLeft() + " " + getRight() + ")";
    }

    @Override
    public String toSql() throws ClassCastException {
        return ((SqlOperation)getLeft()).toSql() +
               " JOIN " + ((SqlOperation)getRight()).toSql() +
               " ON " + getCondition() + " ";
    }

    @Override
    public Dataset<Row> execute(SparkSession spark) {
        return ((CanProduceDataFrame)getLeft()).execute(spark)
                .join(((CanProduceDataFrame)getRight()).execute(spark),
                        functions$.MODULE$.expr(condition));
    }
}
