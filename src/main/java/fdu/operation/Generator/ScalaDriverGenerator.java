package fdu.operation.Generator;

import fdu.operation.Operation;
import fdu.operation.operators.*;

/**
 * Created by slade on 2016/11/24.
 */
public class ScalaDriverGenerator implements OperatorVisitor {
    private String scalaProgram = "";

    @Override
    public void visitDataSource(DataSource source) {

    }

    @Override
    public void visitFilter(Filter filter) {
        Operation op = filter.getLeft();
        if (op instanceof DataSource){
            String alias = ((DataSource) op).getAlias();
            scalaProgram += "from " + ((DataSource) op).toSql() +
                    "where " + filter.getCondition();
        }else if (op instanceof Join){
            scalaProgram += "where " + filter.getCondition();
        }else {
            throw new AssertionError("Not Handled.");
        }
    }

    @Override
    public void visitJoin(Join join) {
        if (! (join.getLeft() instanceof DataSource) || !(join.getRight() instanceof DataSource)){
            throw new AssertionError("Not Handled.");
        }
        DataSource left = (DataSource) join.getLeft();
        DataSource right = (DataSource) join.getRight();
        scalaProgram += "from " + left.toSql() + " join " + right.toSql() + " on " + join.getCondition() + " ";
    }

    @Override
    public void visitKMeans(KMeans kmeans) {

    }

    @Override
    public void visitKMeansModel(KMeansModel model) {
        String sql = scalaProgram;
        scalaProgram = "val df1 = spark.taql(\"" + sql + "\");\n";
        scalaProgram += "val kmeans = new KMeans().setK(" + model.getK() + ").setSeed(1L);\n";
        scalaProgram += "val model = kmeans.fit(df1);\n";
        if ("disk".equals(model.getStrategy())){
            scalaProgram += "model.write.save(\"" + model.getModelName() + "\");";
        }
    }

    @Override
    public void visitProject(Project project) {
        scalaProgram = "select " + project.getProjections() + " " + scalaProgram;
    }

    @Override
    public String generate() {
        return scalaProgram;
    }
}
