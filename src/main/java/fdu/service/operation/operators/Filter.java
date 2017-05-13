/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.service.operation.operators;

import fdu.bean.generator.OperatorVisitor;
import fdu.service.operation.CanProduce;
import fdu.service.operation.SqlOperation;
import fdu.service.operation.UnaryOperation;
import fdu.util.UserSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONObject;

import java.util.Objects;

/**
 *
 * @author Lu Chang
 */
public class Filter extends UnaryOperation implements SqlOperation {
    private final String condition;

    public Filter(String name, String type, String condition) {
        super(name, type);
        this.condition = condition;
    }

    @Override
    public Dataset<Row> execute(UserSession session) {
        return ((CanProduce<Dataset<Row>>) getChild()).executeCached(session).filter(condition);
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        getChild().accept(visitor);
        visitor.visitFilter(this);
    }

    public String getCondition() {
        return condition;
    }

    public static Filter newInstance(JSONObject obj){
        return new Filter(
                obj.getString("name"),
                obj.getString("type"),
                obj.getString("condition"));
    }

    @Override
    public String toString() {
        return "([Filter name: " + name + " condition: " + condition  + "]" + getChild() + ")";
    }

    @Override
    public String toSql() throws ClassCastException {
        SqlOperation left = (SqlOperation) getChild();
        if (left instanceof DataSource)
            return  ((SqlOperation) getChild()).toSql() + " WHERE " + condition;
        else if (left instanceof Join)
            return ((SqlOperation) getChild()).toSql() + " WHERE " + condition;
        else throw new UnsupportedOperationException("Invalid filter node");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        Filter filter = (Filter) o;
        return Objects.equals(condition, filter.condition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), condition);
    }
}
