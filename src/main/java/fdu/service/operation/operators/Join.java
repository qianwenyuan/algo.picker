/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.service.operation.operators;

import fdu.bean.generator.OperatorVisitor;
import fdu.service.operation.BinaryOperation;
import fdu.service.operation.CanProduce;
import fdu.service.operation.SqlOperation;
import fdu.util.UserSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions$;
import org.json.JSONObject;

import java.util.Objects;

/**
 *
 * @author Lu Chang
 */
public class Join extends BinaryOperation implements SqlOperation {
    private final String condition;

    public Join(String name, String type, String condition) {
        super(name, type);
        this.condition = condition;
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        getLeft().accept(visitor);
        getRight().accept(visitor);
        visitor.visitJoin(this);
    }

    public String getCondition() {
        return condition;
    }

    public static Join newInstance(JSONObject obj){
        return new Join(
                obj.getString("name"),
                obj.getString("type"),
                obj.getString("condition"));
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
    public Dataset<Row> execute(UserSession session) {
        return ((CanProduce<Dataset<Row>>)getLeft()).executeCached(session)
                .join(((CanProduce<Dataset<Row>>)getRight()).executeCached(session),
                        functions$.MODULE$.expr(condition));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        Join join = (Join) o;
        return Objects.equals(condition, join.condition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), condition);
    }
}
