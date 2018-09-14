package fdu.bean.generator;

import fdu.service.operation.operators.GroupBy_Count;

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
    void visitGroupBy(GroupBy_Count groupby);

    String generate();
}
