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
 * @author Lu Chang
 */
public class Filter extends UnaryOperation {
    private String name;
    private String condition;

    public Filter(String id, String type, String z) {
        super(id, type, z);
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
}
