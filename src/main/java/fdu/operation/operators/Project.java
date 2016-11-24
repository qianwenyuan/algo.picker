package fdu.operation.operators;

import fdu.operation.Generator.OperatorVisitor;
import fdu.operation.UnaryOperation;
import org.json.JSONObject;

/**
 * Created by slade on 2016/11/23.
 */
public class Project extends UnaryOperation{
    private  String name;
    private String projections;

    public Project(String id, String type, String z) {
        super(id, type, z);
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        getLeft().accept(visitor);
        visitor.visitProject(this);
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setProjections(String projections) {
        this.projections = projections;
    }

    public String getName() {
        return name;
    }

    public String getProjections() {
        return projections;
    }

    public static Project newInstance(JSONObject obj){
        Project result = new Project(obj.getString("id"), obj.getString("type"), obj.getString("z"));
        result.setName(obj.getString("name"));
        result.setProjections(obj.getString("projections"));
        return result;
    }

    @Override
    public String toString() {
        return "([Project: " + projections +"] "+ getLeft() +")";
    }
}
