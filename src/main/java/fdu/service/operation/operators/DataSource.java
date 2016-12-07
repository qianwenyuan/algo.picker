/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.service.operation.operators;

import fdu.bean.generator.OperatorVisitor;
import fdu.service.operation.UnaryOperation;
import org.json.JSONObject;

/**
 *
 * @author slade
 */
public class DataSource extends UnaryOperation {
    private String name;
    private String alias;

    public DataSource(String id, String type, String z) {
        super(id, type, z);
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        visitor.visitDataSource(this);
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getName() {
        return name;
    }

    public String getAlias() {
        return alias;
    }

    //TODO: 如何保证每个operation子类都有这个方法
    public static DataSource newInstance(JSONObject obj){
        DataSource result = new DataSource(obj.getString("id"), obj.getString("type"), obj.getString("z"));
        result.setName(obj.getString("name"));
        result.setAlias(obj.getString("alias"));
        return result;
    }

    @Override
    public String toString() {
        return "([Datasource]: " + name + " as " + alias + ")";
    }

    public String toSql(){
        return name + (((alias == null || alias.length() == 0) ? "" : (" as " + alias)) + " ");
    }
}

