package fdu.bean.generator;

import fdu.service.operation.operators.GroupbyCount;

/**
 * Created by slade on 2016/11/24.
 */
public interface OperatorSourceGenerator  extends OperatorVisitor {
    /*
    @Override
    public void visitCount() {
        Operation op = ；
        scalaProgram += "count"
    }
    */
    void visitGroupbyCount(GroupbyCount groupbyCount);

    String generate();
}
