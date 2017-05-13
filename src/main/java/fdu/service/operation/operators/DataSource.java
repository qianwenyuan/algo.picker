/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.service.operation.operators;

import fdu.bean.generator.OperatorVisitor;
import fdu.service.operation.SqlOperation;
import fdu.service.operation.UnaryOperation;
import fdu.util.UserSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONObject;

import java.util.Objects;

/**
 * @author slade
 */
public class DataSource extends UnaryOperation implements SqlOperation {
    private final String alias;

    public DataSource(String name, String type, String alias) {
        super(name, type);
        this.alias = alias;
    }

    //TODO: 如何保证每个operation子类都有这个方法
    public static DataSource newInstance(JSONObject obj) {
        return new DataSource(
                obj.getString("name"),
                obj.getString("type"),
                obj.getString("alias"));
    }

    @Override
    public Dataset<Row> execute(UserSession user) {
        return user.getSparkSession().table(name).as(alias);
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        visitor.visitDataSource(this);
    }


    public String getAlias() {
        return alias;
    }

    @Override
    public String toString() {
        return "([Datasource]: " + name + " as " + alias + ")";
    }

    @Override
    public String toSql() {
        return name + (((alias == null || alias.length() == 0) ? "" : (" AS " + alias)) + " ");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        DataSource that = (DataSource) o;
        return Objects.equals(alias, that.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), alias);
    }
}

