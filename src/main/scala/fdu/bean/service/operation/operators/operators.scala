package fdu.bean.service.operation.operators

import fdu.bean.generator.OperatorVisitor
import fdu.service.operation._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json.JSONObject

import scala.beans.BeanProperty

/**
  * Created by guoli on 2017/4/26.
  */

trait CanGenFromJson {
  def newInstance(obj: JSONObject): Operation
}
//
//trait CanProduceDataFrame {
//
//  protected var result: Option[(SparkSession, DataFrame)] = None
//
//  abstract def produceResult(spark: SparkSession): DataFrame
//
//  sealed def execute(sparkSession: SparkSession): DataFrame = {
//    result match {
//      case Some((`sparkSession`, dataFrame)) => dataFrame
//      case _ =>
//        val res = produceResult(sparkSession)
//        result = Some(sparkSession, res)
//        res
//    }
//  }
//
//}

class RandomForestModel(id: String,
                        typE: String,
                        z: String,
                        @BeanProperty val name: String,
                        @BeanProperty val numTrees: Int,
                        @BeanProperty val labelCol: String)
  extends UnaryOperation(id, typE, z) {

  override def accept(visitor: OperatorVisitor): Unit = {
    getLeft.accept(visitor)
    visitor.visitRandomForest(this)
  }
}

object RandomForestModel extends CanGenFromJson {

  def unapply(arg: RandomForestModel): Option[(String, Int, String)] = Some((arg.name, arg.numTrees, arg.labelCol))

  override def newInstance(obj: JSONObject): Operation =
    new RandomForestModel(
      name = obj.getString("name"),
      numTrees = obj.getInt("numTrees"),
      labelCol = obj.getString("labelCol"),
      id = obj.getString("id"),
      typE = obj.getString("type"),
      z = obj.getString("z")
    )
}

class RandomForestPredict(id: String,
                          typE: String,
                          z: String,
                          @BeanProperty val name: String)
  extends BinaryOperation(id, typE, z)
  with CanProduceDataFrame {

  private var cachedResult: Option[(SparkSession, DataFrame)] = None

  override def accept(visitor: OperatorVisitor): Unit = {
    getLeft.accept(visitor)
    getRight.accept(visitor)
    visitor.visitRandomForestPredict(this)
  }

  override def execute(spark: SparkSession): DataFrame = {
    cachedResult match {
      case Some((`spark`, dataFrame)) => dataFrame
      case _ =>
        val (modelName, table) =
          getLeft match {
            case RandomForestModel(mName, _, _) => (mName, getRight.asInstanceOf[CanProduceDataFrame].execute(spark))
            case sqlOp: CanProduceDataFrame =>
              val RandomForestModel(name, _, _) = getRight
              (name, sqlOp.asInstanceOf[CanProduceDataFrame].execute(spark))
          }
        val m = org.apache.spark.ml.classification.RandomForestClassificationModel.load(modelName)

        val assembler = new VectorAssembler()
          .setInputCols(table.columns)
          .setOutputCol("features")
        val transformed = assembler.transform(table)
        cachedResult = Some(spark, m.transform(transformed))
        cachedResult.get._2
    }
  }

}

object RandomForestPredict extends CanGenFromJson {
  override def newInstance(obj: JSONObject): Operation =
    new RandomForestPredict(
      name = obj.getString("name"),
      id = obj.getString("id"),
      typE = obj.getString("type"),
      z = obj.getString("z")
    )
}

class LDAModel(id: String,
               typE: String,
               z: String,
               @BeanProperty val name: String,
               @BeanProperty val numTopics: Int,
               @BeanProperty val numMaxIter: Int)
  extends UnaryOperation(id, typE, z) {

  override def accept(visitor: OperatorVisitor): Unit = {
    getLeft.accept(visitor)
    visitor.visitLDA(this)
  }
}

object LDAModel extends CanGenFromJson {
  override def newInstance(obj: JSONObject): Operation =
    new LDAModel(
      name = obj.getString("name"),
      numTopics = obj.getInt("numTopics"),
      numMaxIter = obj.getInt("numMaxIter"),
      id = obj.getString("id"),
      typE = obj.getString("type"),
      z = obj.getString("z")
    )
}

class Word2Vec(id: String,
               typE: String,
               z: String,
               @BeanProperty val name: String,
               @BeanProperty val wordCol: String,
               @BeanProperty val numVecSize: Int)
  extends UnaryOperation(id, typE, z) {

  override def accept(visitor: OperatorVisitor): Unit = {
    getLeft.accept(visitor)
    visitor.visitWord2Vec(this)
  }
}

object Word2Vec extends CanGenFromJson {
  override def newInstance(obj: JSONObject): Operation =
    new Word2Vec(
      name = obj.getString("name"),
      wordCol = obj.getString("wordCol"),
      numVecSize = obj.getInt("numVecSize"),
      id = obj.getString("id"),
      typE = obj.getString("type"),
      z = obj.getString("z")
    )
}