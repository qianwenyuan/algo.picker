package fdu.bean.service.operation.operators

import fdu.bean.generator.OperatorVisitor
import fdu.service.operation._
import fdu.util.UserSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.json.JSONObject

import scala.beans.BeanProperty

/**
  * Created by guoli on 2017/4/26.
  */

class RandomForestModel(name: String,
                        _type: String,
                        @BeanProperty val numTrees: Int,
                        @BeanProperty val labelCol: String)
  extends UnaryOperation(name, _type) {

  override def accept(visitor: OperatorVisitor): Unit = {
    getChild.accept(visitor)
    visitor.visitRandomForest(this)
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
    val (modelName, table) =
      getLeft match {
        case RandomForestModel(mName, _, _) => (mName, getRight.asInstanceOf[CanProduce[DataFrame]].execute(session))
        case sqlOp: CanProduce[DataFrame] =>
          val RandomForestModel(name, _, _) = getRight
          (name, sqlOp.asInstanceOf[CanProduce[DataFrame]].execute(session))
      }
    val m = org.apache.spark.ml.classification.RandomForestClassificationModel.load(modelName)

    val assembler = new VectorAssembler()
      .setInputCols(table.columns)
      .setOutputCol("features")
    val transformed = assembler.transform(table)
    m.transform(transformed)
  }

}

object RandomForestPredict extends CanGenFromJson {
  override def newInstance(obj: JSONObject): Operation =
    new RandomForestPredict(
      name = obj.getString("name"),
      _type = obj.getString("type")
    )
}

class LDAModel(name: String,
               _type: String,
               @BeanProperty val numTopics: Int,
               @BeanProperty val numMaxIter: Int)
  extends UnaryOperation(name, _type) {

  override def accept(visitor: OperatorVisitor): Unit = {
    getChild.accept(visitor)
    visitor.visitLDA(this)
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
  extends UnaryOperation(name, _type) {

  override def accept(visitor: OperatorVisitor): Unit = {
    getChild.accept(visitor)
    visitor.visitWord2Vec(this)
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