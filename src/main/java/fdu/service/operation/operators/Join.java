/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fdu.service.operation.operators;

import fdu.service.operation.BinaryOperation;
import fdu.bean.generator.OperatorVisitor;
import org.json.JSONObject;

/**
 *
 * @author Lu Chang
 */
public class Join extends BinaryOperation {
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
}
