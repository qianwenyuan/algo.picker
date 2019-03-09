package fdu.service.operation.operators

import fdu.bean.generator.OperatorVisitor
import fdu.service.operation._
import fdu.service.operation.operators.executordep.FirstElementFunc
import fdu.util.UserSession
import org.apache.spark.ml._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.feature.{PCA, PCAModel, VectorTransformer}
import org.apache.spark.sql.DataFrame
import org.json.JSONObject
import org.apache.spark.rdd.RDD

import scala.beans.{BeanProperty, BooleanBeanProperty}
import org.apache.spark.sql.functions._

/**
  * Created by guoli on 2017/4/26.
  */

class Sample(name: String,
             _type: String,
             @BooleanBeanProperty val withReplacement: Boolean,
             @BeanProperty val fraction: Double)
  extends UnaryOperation(name, _type)
    with CanProduce[DataFrame] {

  override def execute(user: UserSession): DataFrame = {
    val df = getChild.asInstanceOf[CanProduce[DataFrame]]
    df.executeCached(user).sample(withReplacement, fraction)
  }

  override def accept(visitor: OperatorVisitor): Unit = ??? // Leave Unimplemented

  def canEqual(other: Any): Boolean = other.isInstanceOf[Sample]

  override def equals(other: Any): Boolean = other match {
    case that: Sample =>
      super.equals(that) &&
        (that canEqual this) &&
        withReplacement == that.withReplacement &&
        fraction == that.fraction
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), withReplacement, fraction)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object Sample extends CanGenFromJson {
  override def newInstance(obj: JSONObject): Operation =
    new Sample(
      name = obj.getString("name"),
      _type = obj.getString("type"),
      withReplacement = obj.getBoolean("withReplacement"),
      fraction = obj.getDouble("fraction")
    )
}

class KMeansModel(name: String,
                  _type: String,
                  @BeanProperty val k: Int)
  extends UnaryOperation(name, _type)
    with CanProduce[Model[clustering.KMeansModel]] {
  override def execute(user: UserSession): Model[clustering.KMeansModel] = {
    try
      clustering.KMeansModel.load(getName)
    catch {
      case _: Any =>
        val df = getChild.asInstanceOf[CanProduce[DataFrame]].executeCached(user)
        val kMeans = new clustering.KMeans().setK(getK).setSeed(1L)
        val assembler = new feature.VectorAssembler()
          .setInputCols(df.columns)
          .setOutputCol("features")
        val transformed = assembler.transform(df)
        val trainedModel = kMeans.setFeaturesCol("features").fit(transformed)
        trainedModel
    }
  }

  override def accept(visitor: OperatorVisitor): Unit = {
    getChild.accept(visitor)
    visitor.visitKMeansModel(this)
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[KMeansModel]

  override def equals(other: Any): Boolean = other match {
    case that: KMeansModel =>
      super.equals(that) &&
        (that canEqual this) &&
        k == that.k
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), k)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object KMeansModel extends CanGenFromJson {
  override def newInstance(obj: JSONObject): Operation =
    new KMeansModel(
      obj.getString("name"),
      obj.getString("type"),
      obj.getInt("k"))
}

class VectorAssembler(name: String,
                      _type: String,
                      @BeanProperty val columns: String,
                      @BeanProperty val outputCol: String)
  extends UnaryOperation(name, _type)
    with CanProduce[DataFrame] {

  override def execute(user: UserSession): DataFrame = {
    val df = getChild.asInstanceOf[CanProduce[DataFrame]].executeCached(user)
    val asm = new feature.VectorAssembler()
      .setInputCols(columns.split(",").map(_.trim))
      .setOutputCol(outputCol)
    asm.transform(df)
  }

  override def accept(visitor: OperatorVisitor) = ??? // Leave unimplemented

  override def equals(other: Any): Boolean = other match {
    case that: VectorAssembler =>
      super.equals(that) &&
        (that canEqual this) &&
        columns == that.columns &&
        outputCol == that.outputCol
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[VectorAssembler]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), columns, outputCol)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object VectorAssembler extends CanGenFromJson {
  override def newInstance(obj: JSONObject) = new VectorAssembler(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("columns"),
    obj.getString("outputCol")
  )
}

class RandomForestModel(name: String,
                        _type: String,
                        @BeanProperty val numTrees: Int,
                        @BeanProperty val labelCol: String)
  extends UnaryOperation(name, _type)
    with CanProduce[Model[classification.RandomForestClassificationModel]] {
  override def isNeedCache: Boolean = true
  override def accept(visitor: OperatorVisitor): Unit = {
    getChild.accept(visitor)
    visitor.visitRandomForest(this)
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[RandomForestModel]

  override def equals(other: Any): Boolean = other match {
    case that: RandomForestModel =>
      super.equals(that) &&
        (that canEqual this) &&
        numTrees == that.numTrees &&
        labelCol == that.labelCol
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), numTrees, labelCol)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def execute(user: UserSession): Model[classification.RandomForestClassificationModel] = {
    try
      classification.RandomForestClassificationModel.load(getName)
    catch {
      case _: Any =>
        val df = getChild.asInstanceOf[CanProduce[DataFrame]].executeCached(user)

        val rf = new classification.RandomForestClassifier()
          .setNumTrees(numTrees)
          .setLabelCol(labelCol)
          .setFeaturesCol("features")

        val trainedModel = rf.fit(df)
        trainedModel
    }
  }
}

object RandomForestModel extends CanGenFromJson {

  def unapply(arg: RandomForestModel): Option[(String, Int, String)] = Some((arg.getName, arg.numTrees, arg.labelCol))

  override def newInstance(obj: JSONObject): Operation =
    new RandomForestModel(
      name = obj.getString("name"),
      numTrees = obj.getInt("numTrees"),
      labelCol = obj.getString("labelCol"),
      _type = obj.getString("type")
    )
}

class RandomForestPredict(name: String,
                          _type: String)
  extends BinaryOperation(name, _type)
    with CanProduce[DataFrame] {

  override def accept(visitor: OperatorVisitor): Unit = {
    getLeft.accept(visitor)
    getRight.accept(visitor)
    visitor.visitRandomForestPredict(this)
  }

  override def execute(session: UserSession): DataFrame = {
    val (model, table) =
      getLeft match {
        case m: RandomForestModel =>
          (m.executeCached(session),
            getRight.asInstanceOf[CanProduce[DataFrame]].executeCached(session))
        case t: CanProduce[DataFrame] =>
          (getRight.asInstanceOf[CanProduce[Model[classification.RandomForestClassificationModel]]].executeCached(session),
            t.executeCached(session))
      }

    model.transform(table)
  }

}

object RandomForestPredict extends CanGenFromJson {
  override def newInstance(obj: JSONObject): Operation =
    new RandomForestPredict(
      name = obj.getString("name"),
      _type = obj.getString("type")
    )
}

class LogisticRegressionModel(name: String,
                              _type: String,
                              @BeanProperty val labelCol: String,
                              @BeanProperty val numMaxIter: Int)
  extends UnaryOperation(name, _type)
    with CanProduce[Model[classification.LogisticRegressionModel]] {

  override def execute(user: UserSession): classification.LogisticRegressionModel = {
    try
      classification.LogisticRegressionModel.load(getName)
    catch {
      case _: Any =>
        val df = getChild.asInstanceOf[CanProduce[DataFrame]].executeCached(user)
        val lr = new classification.LogisticRegression()
          .setLabelCol(labelCol)
          .setFeaturesCol("features")
          .setMaxIter(numMaxIter)

        val trainedModel = lr.fit(df)
        trainedModel
    }
  }

  override def accept(visitor: OperatorVisitor) = ??? // Leave unimplemented

  override def equals(other: Any): Boolean = other match {
    case that: LogisticRegressionModel =>
      super.equals(that) &&
        (that canEqual this) &&
        labelCol == that.labelCol &&
        numMaxIter == that.numMaxIter
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[LogisticRegressionModel]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), labelCol, numMaxIter)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object LogisticRegressionModel extends CanGenFromJson {
  override def newInstance(obj: JSONObject): Operation =
    new LogisticRegressionModel(
      name = obj.getString("name"),
      labelCol = obj.getString("labelCol"),
      numMaxIter = obj.getInt("numMaxIter"),
      _type = obj.getString("type")
    )
}

class LogisticRegressionPredict(name: String,
                                _type: String)
  extends BinaryOperation(name, _type)
    with CanProduce[DataFrame] {

  override def accept(visitor: OperatorVisitor): Unit = ???

  override def execute(session: UserSession): DataFrame = {
    // UDF
    //    val probString = "probability"
    //
    //    val transferProbabilityFunc : DenseVector => Double  = _.values.last
    //
    //    def transferProbability(dataFrame: DataFrame): DataFrame = {
    //      if (dataFrame.columns.contains(probString)) {
    //        try {
    //          import org.apache.spark.sql.functions._
    //          dataFrame.withColumn("regularProbability", udf(transferProbabilityFunc).apply(col(probString)))
    //        } finally {
    //          dataFrame
    //        }
    //      } else dataFrame
    //    }

    val (model, table) =
      getLeft match {
        case m: LogisticRegressionModel =>
          (m.executeCached(session),
            getRight.asInstanceOf[CanProduce[DataFrame]].executeCached(session))
        case t: CanProduce[DataFrame] =>
          (getRight.asInstanceOf[CanProduce[Model[classification.LogisticRegressionModel]]].executeCached(session),
            t.executeCached(session))
      }

    // transferProbability(model.transform(table))
    val transformed = model.transform(table)

    // val firstelement = udf((v: Vector) => v(0))
//    session.getSparkSession.udf.register("firstelement", new FirstElementFunc)

     transformed
//    transformed.select(col("*"), callUDF("firstelement", col("probability")).as("prob"))
//      .drop("probability")
//      .drop("features")
//      .drop("rawprediction")
  }

}

object LogisticRegressionPredict extends CanGenFromJson {
  override def newInstance(obj: JSONObject): Operation =
    new LogisticRegressionPredict(
      name = obj.getString("name"),
      _type = obj.getString("type")
    )
}

class LinearRegressionModel(name: String,
                              _type: String,
                              @BeanProperty val labelCol: String,
                              @BeanProperty val numMaxIter: Int)
  extends UnaryOperation(name, _type)
    with CanProduce[Model[classification.LogisticRegressionModel]] {

  override def execute(user: UserSession): classification.LogisticRegressionModel = {
    try
      classification.LogisticRegressionModel.load(getName)
    catch {
      case _: Any =>
        val df = getChild.asInstanceOf[CanProduce[DataFrame]].executeCached(user)
        val lr = new classification.LogisticRegression()
          .setLabelCol(labelCol)
          .setFeaturesCol("features")
          .setMaxIter(numMaxIter)

        val trainedModel = lr.fit(df)
        trainedModel
    }
  }

  override def accept(visitor: OperatorVisitor) = ??? // Leave unimplemented

  override def equals(other: Any): Boolean = other match {
    case that: LogisticRegressionModel =>
      super.equals(that) &&
        (that canEqual this) &&
        labelCol == that.labelCol &&
        numMaxIter == that.numMaxIter
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[LogisticRegressionModel]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), labelCol, numMaxIter)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object LinearRegressionModel extends CanGenFromJson {
  override def newInstance(obj: JSONObject): Operation =
    new LogisticRegressionModel(
      name = obj.getString("name"),
      labelCol = obj.getString("labelCol"),
      numMaxIter = obj.getInt("numMaxIter"),
      _type = obj.getString("type")
    )
}

class LinearRegressionPredict(name: String,
                                _type: String)
  extends BinaryOperation(name, _type)
    with CanProduce[DataFrame] {

  override def accept(visitor: OperatorVisitor): Unit = ???

  override def execute(session: UserSession): DataFrame = {
    // UDF
    //    val probString = "probability"
    //
    //    val transferProbabilityFunc : DenseVector => Double  = _.values.last
    //
    //    def transferProbability(dataFrame: DataFrame): DataFrame = {
    //      if (dataFrame.columns.contains(probString)) {
    //        try {
    //          import org.apache.spark.sql.functions._
    //          dataFrame.withColumn("regularProbability", udf(transferProbabilityFunc).apply(col(probString)))
    //        } finally {
    //          dataFrame
    //        }
    //      } else dataFrame
    //    }

    val (model, table) =
      getLeft match {
        case m: LogisticRegressionModel =>
          (m.executeCached(session),
            getRight.asInstanceOf[CanProduce[DataFrame]].executeCached(session))
        case t: CanProduce[DataFrame] =>
          (getRight.asInstanceOf[CanProduce[Model[classification.LogisticRegressionModel]]].executeCached(session),
            t.executeCached(session))
      }

    // transferProbability(model.transform(table))
    val transformed = model.transform(table)

//    val firstelement = udf(new FirstElementFunc)

     transformed
//    transformed.withColumn("prob", firstelement(col("probability")))
//      .drop("probability")
//      .drop("features")
//      .drop("rawprediction")
  }

}

object LinearRegressionPredict extends CanGenFromJson {
  override def newInstance(obj: JSONObject): Operation =
    new LogisticRegressionPredict(
      name = obj.getString("name"),
      _type = obj.getString("type")
    )
}

class OneHotEncoder(name: String,
                    _type: String,
                    @BeanProperty val features: String)
  extends UnaryOperation(name, _type)
    with CanProduce[DataFrame] {

  override def execute(user: UserSession): DataFrame = {
    val child = getChild.asInstanceOf[CanProduce[DataFrame]].executeCached(user)
    val featureArray = features.split(",").map(_.trim)
    val colList = scala.collection.mutable.ListBuffer.empty[String]

    val encoders = featureArray.map(s => {
      val colName = s"$s-encoded"
      colList += colName
      new feature.OneHotEncoder()
        .setInputCol(s)
        .setOutputCol(colName)
    })
    // .foldLeft(child)((table, encoder) => encoder.transform(table))
    val pipeline = new Pipeline().setStages(encoders)
    val encoded = pipeline.fit(child).transform(child)
    val colArray = colList.toArray
    val assembler = new feature.VectorAssembler()
      .setInputCols(colArray)
      .setOutputCol("features")
    val result = assembler.transform(encoded)
    colList.foldLeft(result)((tbl, colName) => tbl.drop(colName))
  }

  override def accept(visitor: OperatorVisitor) = ??? // Leave unimplemented

  override def equals(other: Any): Boolean = other match {
    case that: OneHotEncoder =>
      super.equals(that) &&
        (that canEqual this) &&
        features == that.features
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[OneHotEncoder]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), features)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object OneHotEncoder extends CanGenFromJson {
  override def newInstance(obj: JSONObject) = new OneHotEncoder(
    name = obj.getString("name"),
    _type = obj.getString("type"),
    features = obj.getString("features")
  )
}

class LDAModel(name: String,
               _type: String,
               @BeanProperty val numTopics: Int,
               @BeanProperty val numMaxIter: Int)
  extends UnaryOperation(name, _type)
    with CanProduce[Model[clustering.LDAModel]] {

  override def accept(visitor: OperatorVisitor): Unit = {
    getChild.accept(visitor)
    visitor.visitLDA(this)
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[LDAModel]

  override def equals(other: Any): Boolean = other match {
    case that: LDAModel =>
      super.equals(that) &&
        (that canEqual this) &&
        numTopics == that.numTopics &&
        numMaxIter == that.numMaxIter
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), numTopics, numMaxIter)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def execute(user: UserSession): Model[clustering.LDAModel] = {
    try
      clustering.LocalLDAModel.load(getName)
    catch {
      case _: Any =>
        try
          clustering.DistributedLDAModel.load(getName)
        catch {
          case _: Any =>
            val df = getChild.asInstanceOf[CanProduce[DataFrame]].executeCached(user)
            val assembler = new feature.VectorAssembler()
              .setInputCols(df.columns)
              .setOutputCol("features")
            val transformed = assembler.transform(df)

            val lda = new clustering.LDA()
              .setK(numTopics)
              .setMaxIter(numMaxIter)
              .setFeaturesCol("features")

            val trainedModel = lda.fit(transformed)
            trainedModel
        }
    }
  }
}

object LDAModel extends CanGenFromJson {
  override def newInstance(obj: JSONObject): Operation =
    new LDAModel(
      name = obj.getString("name"),
      numTopics = obj.getInt("numTopics"),
      numMaxIter = obj.getInt("numMaxIter"),
      _type = obj.getString("type")
    )
}

class Word2Vec(name: String,
               _type: String,
               @BeanProperty val wordCol: String,
               @BeanProperty val numVecSize: Int)
  extends UnaryOperation(name, _type)
    with CanProduce[Model[feature.Word2VecModel]] {

  override def accept(visitor: OperatorVisitor): Unit = {
    getChild.accept(visitor)
    visitor.visitWord2Vec(this)
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Word2Vec]

  override def equals(other: Any): Boolean = other match {
    case that: Word2Vec =>
      super.equals(that) &&
        (that canEqual this) &&
        wordCol == that.wordCol &&
        numVecSize == that.numVecSize
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), wordCol, numVecSize)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def execute(user: UserSession): Model[org.apache.spark.ml.feature.Word2VecModel] = {
    try
      feature.Word2VecModel.load(getName)
    catch {
      case _: Any =>
        val df = getChild.asInstanceOf[CanProduce[DataFrame]].executeCached(user)

        val word2Vec = new org.apache.spark.ml.feature.Word2Vec()
          .setInputCol(wordCol)
          .setOutputCol(s"$wordCol result")
          .setVectorSize(numVecSize)

        val word2VecModel = word2Vec.fit(df)
        // val result = word2VecModel.transform(df)
        word2VecModel
    }
  }
}

object Word2Vec extends CanGenFromJson {
  override def newInstance(obj: JSONObject): Operation =
    new Word2Vec(
      name = obj.getString("name"),
      wordCol = obj.getString("wordCol"),
      numVecSize = obj.getInt("numVecSize"),
      _type = obj.getString("type")
    )
}

//TODO
class NaiveBayesModel(name: String,
                      _type: String,
                      @BeanProperty val label: String,
                      @BeanProperty val smoothing: Double,
                      @BeanProperty val modelType: String)
  extends UnaryOperation(name, _type)
    with CanProduce[Model[classification.NaiveBayesModel]] {

  override def execute(user: UserSession): classification.NaiveBayesModel = {
    try
      classification.NaiveBayesModel.load(getName)
    catch {
      case _: Any =>
        val df = getChild.asInstanceOf[CanProduce[DataFrame]].executeCached(user)
        val nb = new classification.NaiveBayes()
          .setLabelCol(label)
          .setFeaturesCol("features")
          .setSmoothing(smoothing)
          .setModelType(modelType)

        val trainedModel = nb.fit(df)
        trainedModel
    }
  }

  override def accept(visitor: OperatorVisitor) = ??? // Leave unimplemented

  override def equals(other: Any): Boolean = other match {
    case that: NaiveBayesModel =>
      super.equals(that) &&
        (that canEqual this) &&
        label == that.label &&
        smoothing == that.smoothing &&
        modelType == that.modelType
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[NaiveBayesModel]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), label, smoothing, modelType)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object NaiveBayesModel extends CanGenFromJson {
  override def newInstance(obj: JSONObject): Operation =
    new NaiveBayesModel(
      name = obj.getString("name"),
      label = obj.getString("label"),
      smoothing = obj.getDouble("smoothing"),
      modelType = obj.getString("modelType"),
      _type = obj.getString("type")
    )
}

class NaiveBayesPredict(name: String,
                        _type: String)
  extends BinaryOperation(name, _type)
    with CanProduce[DataFrame] {

  override def accept(visitor: OperatorVisitor): Unit = ???

  override def execute(session: UserSession): DataFrame = {
    val (model, table) =
      getLeft match {
        case m: NaiveBayesModel =>
          (m.executeCached(session),
            getRight.asInstanceOf[CanProduce[DataFrame]].executeCached(session))
        case t: CanProduce[DataFrame] =>
          (getRight.asInstanceOf[CanProduce[Model[classification.NaiveBayesModel]]].executeCached(session),
            t.executeCached(session))
      }
    model.transform(table)
  }
}

object NaiveBayesPredict extends CanGenFromJson {
  override def newInstance(obj: JSONObject): Operation =
    new NaiveBayesPredict(
      name = obj.getString("name"),
      _type = obj.getString("type")
    )
}

class DecisionTreeClassificationModel(name: String,
                                    _type: String,
                                    @BeanProperty val labelColumn: String)
  extends UnaryOperation(name, _type)
    with CanProduce[Model[classification.DecisionTreeClassificationModel]] {

  override def execute(user: UserSession): classification.DecisionTreeClassificationModel = {
    try
      classification.DecisionTreeClassificationModel.load(getName)
    catch {
      case _: Any =>
        val df = getChild.asInstanceOf[CanProduce[DataFrame]].executeCached(user)
        val nb = new classification.DecisionTreeClassifier()
          .setLabelCol(labelColumn)
          .setFeaturesCol("features")
          .setImpurity("entropy")

        val trainedModel = nb.fit(df)
        trainedModel
    }
  }

  override def accept(visitor: OperatorVisitor) = ??? // Leave unimplemented

  override def equals(other: Any): Boolean = other match {
    case that: DecisionTreeClassificationModel =>
      super.equals(that) &&
        (that canEqual this) &&
        labelColumn == that.labelColumn
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[NaiveBayesModel]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), labelColumn)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object DecisionTreeClassificationModel extends CanGenFromJson {
  override def newInstance(obj: JSONObject): Operation =
    new DecisionTreeClassificationModel(
      name = obj.getString("name"),
      labelColumn = obj.getString("labelColumn"),
      _type = obj.getString("type")
    )
}

class DecisionTreeClassificationPredict(name: String,
                                        _type: String)
  extends BinaryOperation(name, _type)
    with CanProduce[DataFrame] {

  override def accept(visitor: OperatorVisitor): Unit = ???

  override def execute(session: UserSession): DataFrame = {
    val (model, table) =
      getLeft match {
        case m: DecisionTreeClassificationModel =>
          (m.executeCached(session),
            getRight.asInstanceOf[CanProduce[DataFrame]].executeCached(session))
        case t: CanProduce[DataFrame] =>
          (getRight.asInstanceOf[CanProduce[Model[classification.DecisionTreeClassificationModel]]].executeCached(session),
            t.executeCached(session))
      }
    model.transform(table)
  }
}

object DecisionTreeClassificationPredict extends CanGenFromJson {
  override def newInstance(obj: JSONObject): Operation =
    new DecisionTreeClassificationPredict(
      name = obj.getString("name"),
      _type = obj.getString("type")
    )
}

class PCAModel(name: String,
          _type: String,
          @BeanProperty val k: Int)
  extends UnaryOperation(name, _type)
    with CanProduce[DataFrame] {

  override def execute(user: UserSession): DataFrame = {
    val df = getChild.asInstanceOf[CanProduce[DataFrame]].executeCached(user)
    val data = {
      df.asInstanceOf[RDD[org.apache.spark.mllib.regression.LabeledPoint]]
    }


    val trainedModel = {
      new PCA(k).fit(data.map(_.features))
    }
    trainedModel.toString().asInstanceOf[DataFrame]
  }

  override def accept(visitor: OperatorVisitor) = ??? // Leave unimplemented

  override def equals(other: Any): Boolean = other match {
    case that: PCAModel =>
      super.equals(that) &&
        (that canEqual this) &&
        k == that.k
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[PCAModel]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), k)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

