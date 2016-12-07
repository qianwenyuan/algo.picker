package fdu.bean.generator;

import fdu.service.operation.Operation;
import fdu.service.operation.operators.*;
import org.springframework.context.annotation.Bean;

/**
 * Created by slade on 2016/11/24.
 */
public class ScalaDriverGenerator implements OperatorVisitor {
    private String scalaProgram = "";
    private final String importTaqlImplicit = "import org.apache.spark.sql.TAQLImplicits._\n";
    private final String importKmeans = "import org.apache.spark.ml.clustering.KMeans\n";
    private final String importVectorAssember = "import org.apache.spark.ml.feature.VectorAssembler\n";
    private final String importVectors = "import org.apache.spark.ml.linalg.Vectors\n";

    @Override
    public void visitDataSource(DataSource source) {

    }

    @Override
    public void visitFilter(Filter filter) {
        Operation op = filter.getLeft();
        if (op instanceof DataSource) {
            String alias = ((DataSource) op).getAlias();
            scalaProgram += "from " + ((DataSource) op).toSql() +
                    "where " + filter.getCondition();
        } else if (op instanceof Join) {
            scalaProgram += "where " + filter.getCondition();
        } else {
            throw new AssertionError("Not Handled.");
        }
    }

    @Override
    public void visitJoin(Join join) {
        if (!(join.getLeft() instanceof DataSource) || !(join.getRight() instanceof DataSource)) {
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
        scalaProgram = "val df1 = spark.taql(\"" + sql + ";\");\n";
        scalaProgram += importKmeans;
        scalaProgram += "val kmeans = new KMeans().setK(" + model.getK() + ").setSeed(1L);\n";
        scalaProgram += importVectorAssember;
        scalaProgram += importVectors;
        if (!(model.getLeft() instanceof Project)) {
            throw new AssertionError("a project node is required before kmeans node");
        }
        Project projectNode = (Project) model.getLeft();
        scalaProgram += "val assembler = new VectorAssembler().setInputCols(Array(" + projectNode.getFormatedProjections() + ")).setOutputCol(\"features\")\n";
        scalaProgram += "val output = assembler.transform(df1)\n";
        scalaProgram += "val output1 = output.select(\"features\")\n";
        scalaProgram += "val model = kmeans.fit(output1);\n";
        if ("disk".equals(model.getStrategy())) {
            scalaProgram += "model.write.save(\"" + model.getModelName() + "\");";
        }
    }

    @Override
    public void visitProject(Project project) {
        scalaProgram = "select " + project.getProjections() + " " + scalaProgram;
    }

    @Override
    public String generate() {
        return importTaqlImplicit + scalaProgram;
    }
}
