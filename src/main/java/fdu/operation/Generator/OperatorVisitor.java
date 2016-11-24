package fdu.operation.Generator;

import fdu.operation.operators.*;

/**
 * Created by slade on 2016/11/24.
 */
public interface OperatorVisitor {
    void visitDataSource(DataSource source);

    void visitFilter(Filter filter);

    void visitJoin(Join join);

    void visitKMeans(KMeans kmeans);

    void visitKMeansModel(KMeansModel model);

    void visitProject(Project project);

    String generate();
}
