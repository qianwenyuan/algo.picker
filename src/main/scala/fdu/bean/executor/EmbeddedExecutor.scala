package fdu.bean.executor

import java.io.OutputStream

import fdu.bean.generator.LocalVisitor
import fdu.util.UserSession
import org.apache.spark.sql.SparkSession

import scala.tools.nsc.interpreter.Results
import scala.util.Properties.{javaVersion, javaVmName, versionString}

class EmbeddedExecutor(session: UserSession, out: OutputStream) {

  val spark: SparkSession = SparkSession
      .builder
      .appName(s"AlgoPicker - Session: ${session.getSessionID}")
      .enableHiveSupport()
      .getOrCreate()

  lazy val executor: LocalVisitor = new LocalVisitor(session)

  private lazy val repl: IntpREPLRunner = {
    val r = new IntpREPLRunner(out)
    initRepl(r)
    r
  }

  def executeCommand(s: String): Results.Result = repl.execute(s)

  def eval(s: String): AnyRef = repl.eval(s)

  def init(): Unit = {
    initData()
  }

  private def importData(tableName: String): Unit = {
    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", value = true)
      .option("inferSchema", "true")
      // .load(s"file:///c:/$tableName.csv")
      .load(s"spadata/$tableName.csv")
    df.createOrReplaceTempView(s"${tableName}_view")
    spark.sql(s"create table $tableName as select * from ${tableName}_view")
    spark.sqlContext.dropTempTable(s"${tableName}_view")
  }

  private def initData(): Unit = {
    // val tableNames = "info" :: "ad" :: Nil
    // tableNames.foreach(importData)
    // Preprocessing
    /* spark.sql(
      """
        |create table info as
        |(select label,clickTime,creativeID,userID,positionID,connectionType,telecomsOperator from test)
        |union
        |(select label,conversionTime,creativeID,userID,positionID,connectionType,telecomsOperator from train)
      """.stripMargin) */

    /* spark.sql(
      """

        |(select 标签,点击时间,素材ID,用户ID,广告位ID,联网方式,运营商 from test)
        |union
        |(select 标签,点击时间,素材ID,用户ID,广告位ID,联网方式,运营商 from train))
      """.stripMargin) */

    // |select * from (
    // // Add data source for demo
//    val userDf = spark.read.format("CSV").option("header", "true")
//      .schema(StructType(List(
//        StructField("userID", IntegerType),
//        StructField("age", IntegerType),
//        StructField("gender", IntegerType),
//        StructField("education", IntegerType),
//        StructField("marriageStatus", IntegerType),
//        StructField("haveBaby", IntegerType),
//        StructField("hometown", IntegerType),
//        StructField("residence", IntegerType)
//      )))
//      // .load("hdfs://10.141.211.91:9000/user/scidb/liangchg/user.csv")
//      .load("/mnt/c/user.csv")
//      // .load(getClass.getClassLoader.getResource("user.csv").toExternalForm)
//    userDf.createOrReplaceTempView("user_view")
//
//    val actionDf = spark.read.format("CSV").option("header", "true")
//      .schema(StructType(List(
//        StructField("userID", IntegerType),
//        StructField("installTime", IntegerType),
//        StructField("appID", IntegerType)
//      )))
//      // .load("hdfs://10.141.211.91:9000/user/scidb/liangchg/user_app_actions.csv")
//      .load("/mnt/c/user_app_actions.csv")
//      // .load(getClass.getClassLoader.getResource("user_app_actions.csv").toExternalForm)
//    actionDf.createOrReplaceTempView("action_view")
//
//    // Hive test
//    import spark.sql
//     sql("create table user as select * from user_view")
//     sql("create table action as select * from action_view")
    // sql("SELECT * FROM user").show()
  }

  import org.apache.spark.sql.Dataset
  implicit class REPLDataFrame[T](ds: Dataset[T]) {
    def show(): Unit = {
      ds.show()
    }
  }

  private def initRepl(repl : IntpREPLRunner): Unit = {
    repl.bind("spark", spark)
    repl.bind("sc", spark.sparkContext)
    printWelcome(repl)
    repl.execute(
      """
        |import org.apache.spark.SparkContext._
        |import spark.implicits._
        |import spark.sql
        |import org.apache.spark.sql.functions._
      """.stripMargin)
  }

  /** Print a welcome message */
  def printWelcome(repl : IntpREPLRunner) {
    import org.apache.spark.SPARK_VERSION
    repl.echo("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
         """.format(SPARK_VERSION))
    val welcomeMsg = "Using Scala %s (%s, Java %s)".format(
      versionString, javaVmName, javaVersion)
    repl.echo(welcomeMsg)
    repl.echo("Type in expressions to have them evaluated.")
  }

  def tableExists(table: String): Boolean = getTableNames.contains(table)

  def getTableNames: Array[String] = spark.catalog.listTables().collect().map(_.name)

  def getTableSchemas(tableNames: Array[String]): Array[(String, String)] = tableNames.filter(_.length > 0).flatMap {
    n => getTableSchema(n) match {
      case Some(schema) => Some(n, schema)
      case _ => None
    }
  }

  private def getTableSchema(tableName: String): Option[String] = {
    if (spark.catalog.listTables().collect().map(_.name).contains(tableName))
      Some(spark.table(tableName).schema.json)
    else None
  }

  def destroy(): Unit = spark.stop()
}