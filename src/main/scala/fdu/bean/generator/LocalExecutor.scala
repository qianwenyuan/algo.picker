package fdu.bean.generator

import fdu.bean.service.operation.operators.{LDAModel, RandomForestModel, RandomForestPredict, Word2Vec}
import fdu.service.operation.SqlOperation
import fdu.service.operation.operators.{DataSource, _}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * This driver generator will always use sql to produce data used by
  * ML algorithms like KMeans.
  * A basic sql includes data source, project, filter and join
  *
  * Created by Liangchen on 2017/4/5.
  */

class LocalExecutor(spark: SparkSession) extends OperatorVisitor {

  @deprecated
  private val sql: StringBuilder = StringBuilder.newBuilder

  val report: StringBuilder = StringBuilder.newBuilder

  private def log(s: => String): Unit = {
    report ++= s
    println(s)
  }

  private def fetchDataSet(sqlOperation: SqlOperation): Dataset[_] = {
    log {
      s"""
         |SQL: ${sqlOperation.toSql}
         |
        """.stripMargin
    }
    // spark.sql(sqlOperation.toSql)
    sqlOperation.execute(spark)
  }

  @deprecated
  def visitDataSource(source: DataSource): Unit = () // Do nothing

  @deprecated
  def visitFilter(filter: Filter): Unit = {
    filter.getLeft match {
      case op if op.isInstanceOf[DataSource] =>
        sql ++= " from " + op.asInstanceOf[DataSource].toSql
        " where " + filter.getCondition + " "
      case op if op.isInstanceOf[Join] =>
        sql ++= " where " + filter.getCondition
      case _ =>
        throw new AssertionError("Not handled")
    }
  }

  @deprecated
  def visitJoin(join: Join): Unit = {
    require(join.getLeft.isInstanceOf[DataSource] && join.getRight.isInstanceOf[DataSource],
      "Not handled"
    )
    sql ++= " from " + join.getLeft.asInstanceOf[DataSource].toSql +
      " join " + join.getRight.asInstanceOf[DataSource].toSql +
      " on " + join.getCondition + " "
  }

  @deprecated
  def visitProject(project: Project): Unit = {
    sql.insert(0, "select " + project.getProjections + " ")
  }

  def visitKMeansModel(model: KMeansModel): Unit = {
    import org.apache.spark.ml.clustering.KMeans
    import org.apache.spark.ml.feature.VectorAssembler

    val completeModel =
      try
        org.apache.spark.ml.clustering.KMeansModel.load(model.getModelName)
      catch {
        case _: Any =>
          val df = fetchDataSet(model.getLeft.asInstanceOf[SqlOperation])
          val kMeans = new KMeans().setK(model.getK).setSeed(1L)
          val assembler = new VectorAssembler()
            .setInputCols(df.columns)
            .setOutputCol("features")
          val transformed = assembler.transform(df)
          val output = transformed.select("features")
          val trainedModel = kMeans.fit(output)
          trainedModel.write.save(model.getModelName)
          trainedModel
      }
    log {
      s"""
         |KMeans:
         |${
        try completeModel.summary catch {
          case _: Any => completeModel.clusterCenters
        }
      }
      """.stripMargin
    }
  }

  override def visitRandomForest(model: RandomForestModel): Unit = {
    val completeModel =
      try
        org.apache.spark.ml.classification.RandomForestClassificationModel.load(model.name)
      catch {
        case _: Any =>
          val df = fetchDataSet(model.getLeft.asInstanceOf[SqlOperation])

          val assembler = new VectorAssembler()
            .setInputCols(df.columns.filter(_ != model.labelCol))
            .setOutputCol("features")
          val transformed = assembler.transform(df)

          val rf = new RandomForestClassifier()
            .setNumTrees(model.numTrees)
            .setLabelCol(model.labelCol)
            .setFeaturesCol("features")

          val trainedModel = rf.fit(transformed)
          trainedModel.save(model.name)
          trainedModel
      }
    log {
      s"""
         |RandomForest:
         |${completeModel.toDebugString}
         |
          """.stripMargin
    }
  }

  @deprecated
  override def visitRandomForestPredict(predict: RandomForestPredict): Unit = {
    val (modelName, dataFrameSql) =
      predict.getLeft match {
        case RandomForestModel(name, _, _) => (name, predict.getRight.asInstanceOf[SqlOperation])
        case sqlOp: SqlOperation =>
          val RandomForestModel(name, _, _) = predict.getRight
          (name, sqlOp.asInstanceOf[SqlOperation])
      }
    val m = org.apache.spark.ml.classification.RandomForestClassificationModel.load(modelName)
    val table = fetchDataSet(dataFrameSql)

    val assembler = new VectorAssembler()
      .setInputCols(table.columns)
      .setOutputCol("features")
    val transformed = assembler.transform(table)

    val result = m.transform(transformed)

    log {
      s"""
        |RandomForestPredict:
        |Model Name: $modelName
        |Predict result: $result
      """.stripMargin
    }
    result.show()
  }

  override def visitLDA(model: LDAModel): Unit = {
    val completeModel =
      try
        org.apache.spark.ml.clustering.LocalLDAModel.load(model.name)
      catch {
        case _: Any =>
          try
            org.apache.spark.ml.clustering.DistributedLDAModel.load(model.name)
          catch {
            case _: Any =>
              val df = fetchDataSet(model.getLeft.asInstanceOf[SqlOperation])
              val assembler = new VectorAssembler()
                .setInputCols(df.columns)
                .setOutputCol("features")
              val transformed = assembler.transform(df)

              val lda = new LDA()
                .setK(model.numTopics)
                .setMaxIter(model.numMaxIter)

              val trainedModel = lda.fit(transformed)
              trainedModel.save(model.name)
              trainedModel
          }
      }

    log {
      s"""
         |LDA:
         |${
        completeModel
          .describeTopics().toJSON.collect()
          .foldLeft("")(_ + _ + "\n")
      }
         |
          """.stripMargin
    }
  }

  override def visitWord2Vec(model: Word2Vec): Unit = {
    val completeModel =
      try
        org.apache.spark.ml.feature.Word2VecModel.load(model.name)
      catch {
        case _: Any =>
          val df = fetchDataSet(model.getLeft.asInstanceOf[SqlOperation])

          val word2Vec = new org.apache.spark.ml.feature.Word2Vec()
          .setInputCol(model.wordCol)
          .setOutputCol(s"${model.wordCol} result")
          .setVectorSize(model.numVecSize)

          val word2VecModel = word2Vec.fit(df)
          // val result = word2VecModel.transform(df)
          word2VecModel
      }

    log {
      s"""
         |Word2vec:
         |${completeModel.getVectors}
         |
          """.stripMargin
    }
  }

}