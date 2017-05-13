package fdu.service.operation.operators;


import fdu.bean.generator.OperatorVisitor;
import fdu.service.operation.UnaryOperation;
import org.json.JSONObject;

import java.util.Objects;

/**
 * Created by slade on 2016/11/16.
 */
public class KMeansModel extends UnaryOperation {
    private final int k;

    public KMeansModel(String name, String type, int k) {
        super(name, type);
        this.k = k;
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        getChild().accept(visitor);
        visitor.visitKMeansModel(this);
    }

    public int getK() {
        return k;
    }

    public static KMeansModel newInstance(JSONObject obj){
        return new KMeansModel(obj.getString("name"), obj.getString("type"), obj.getInt("k"));
    }

    @Override
    public String toString() {
        return "([KMeansModel k:" + k + "]" + getChild() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        KMeansModel that = (KMeansModel) o;
        return k == that.k;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), k);
    }
}