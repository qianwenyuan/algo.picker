package fdu.bean.generator;

import fdu.service.operation.Operation;
import fdu.service.operation.operators.*;

/**
 * Created by slade on 2016/11/24.
 */
public class ScalaDriverGenerator implements OperatorSourceGenerator {
    private String scalaProgram = "";
    private final String importTaqlImplicit = "import org.apache.spark.sql.TAQLImplicits._\n";
    private final String importKmeans = "import org.apache.spark.ml.clustering.KMeans\n";
    private final String importVectorAssember = "import org.apache.spark.ml.feature.VectorAssembler\n";
    private final String importVectors = "import org.apache.spark.ml.linalg.Vectors\n";

    @Override
    public void visitDataSource(DataSource source) {

    }

    /*
    @Override
    public void visitCount() {
        Operation op = ；
        scalaProgram += "count"
    }
    */
    @Override
    public void visitGroupbyCount(GroupbyCount groupbyCount) {
        Operation op = groupbyCount.getChild();
        if (op instanceof DataSource) {
            String alias = ((DataSource) op).getAlias();
            scalaProgram += "from " + ((DataSource) op).toSql() +
                    "groupby " + groupbyCount.getColumn() + " count";
        } else if (op instanceof Join) {
            scalaProgram += "groupby " + groupbyCount.getColumn()+" count";
        } else {
            throw new AssertionError("Not Handled.");
        }
    }

    @Override
    public void visitFilter(Filter filter) {
        Operation op = filter.getChild();
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
    public void visitKMeansModel(KMeansModel model) {
        String sql = scalaProgram;
        scalaProgram = "val df1 = spark.taql(\"" + sql + ";\");\n";
        scalaProgram += importKmeans;
        scalaProgram += "val kmeans = new KMeans().setK(" + model.getK() + ").setSeed(1L);\n";
        scalaProgram += importVectorAssember;
        scalaProgram += importVectors;
        if (!(model.getChild() instanceof Project)) {
            throw new AssertionError("a project node is required before kmeans node");
        }
        Project projectNode = (Project) model.getChild();
        scalaProgram += "val assembler = new VectorAssembler().setInputCols(Array(" + projectNode.getFormattedProjections() + ")).setOutputCol(\"features\")\n";
        scalaProgram += "val output = assembler.transform(df1)\n";
        scalaProgram += "val output1 = output.select(\"features\")\n";
        scalaProgram += "val model = kmeans.fit(output1);\n";
        scalaProgram += "model.write.save(\"" + model.getName() + "\");";
    }

    @Override
    public void visitProject(Project project) {
        scalaProgram = "select " + project.getProjections() + " " + scalaProgram;
    }

    @Override
    public void visitRandomForest(RandomForestModel model) {
        // TODO
    }

    @Override
    public void visitRandomForestPredict(RandomForestPredict predict) {
        // TODO
    }

    @Override
    public void visitLDA(LDAModel model) {
        // TODO
    }

    @Override
    public void visitWord2Vec(Word2Vec model) {
        //TODO
    }

    @Override
    public String generate() {
        return importTaqlImplicit + scalaProgram;
    }
}
