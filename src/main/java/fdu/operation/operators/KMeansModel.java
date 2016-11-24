package fdu.operation.operators;


import fdu.operation.Generator.OperatorVisitor;
import fdu.operation.UnaryOperation;
import org.json.JSONObject;

/**
 * Created by slade on 2016/11/16.
 */
public class KMeansModel extends UnaryOperation {
    private String name;
    private int k;
    private String modelName;
    private String strategy;

    public KMeansModel(String id, String type, String z) {
        super(id, type, z);
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        getLeft().accept(visitor);
        visitor.visitKMeansModel(this);
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setK(int k) {
        this.k = k;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    public String getName() {
        return name;
    }

    public int getK() {
        return k;
    }

    public String getModelName() {
        return modelName;
    }

    public String getStrategy() {
        return strategy;
    }

    public static KMeansModel newInstance(JSONObject obj){
        KMeansModel result = new KMeansModel(obj.getString("id"), obj.getString("type"), obj.getString("z"));
        result.setName(obj.getString("name"));
        result.setK(obj.getInt("k"));
        result.setModelName(obj.getString("modelName"));
        result.setStrategy(obj.getString("strategy"));
        return result;
    }

    @Override
    public String toString() {
        return "([KMeansModel k:" + k + " modelName: " + modelName + " strategy: " + strategy +"]" + getLeft() + ")";
    }
}