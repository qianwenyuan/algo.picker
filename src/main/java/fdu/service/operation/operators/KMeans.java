package fdu.service.operation.operators;

import fdu.bean.generator.OperatorVisitor;
import fdu.service.operation.UnaryOperation;

/**
 * Created by slade on 2016/11/24.
 */
public class KMeans extends UnaryOperation {

    private String modelName;
    private String strategy;

    public KMeans(String id, String type, String z) {
        super(id, type, z);
    }

    @Override
    public void accept(OperatorVisitor visitor) {
        getLeft().accept(visitor);
        visitor.visitKMeans(this);
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    public String getModelName() {
        return modelName;
    }

    public String getStrategy() {
        return strategy;
    }
}
