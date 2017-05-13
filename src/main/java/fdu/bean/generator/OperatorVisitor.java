package fdu.bean.generator;

import fdu.bean.service.operation.operators.LDAModel;
import fdu.bean.service.operation.operators.RandomForestModel;
import fdu.bean.service.operation.operators.RandomForestPredict;
import fdu.bean.service.operation.operators.Word2Vec;
import fdu.service.operation.operators.*;

/**
 * Created by guoli on 2017/4/5.
 */
@Deprecated
public interface OperatorVisitor {

    void visitDataSource(DataSource source);

    void visitFilter(Filter filter);

    void visitJoin(Join join);

    void visitKMeansModel(KMeansModel model);

    void visitRandomForest(RandomForestModel model);

    void visitRandomForestPredict(RandomForestPredict predict);

    void visitLDA(LDAModel model);

    void visitProject(Project project);

    void visitWord2Vec(Word2Vec model);
}
