package fdu.bean.executor

import java.io.OutputStream

import fdu.bean.generator.LocalVisitor
import fdu.util.UserSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.tools.nsc.interpreter.Results
import scala.util.Properties.{javaVersion, javaVmName, versionString}

class EmbeddedExecutor(session: UserSession, out: OutputStream, master: String = "local[*]") {

  lazy val spark: SparkSession = SparkSession
      .builder
      .appName(s"AlgoPicker - Session: ${session.getSessionID}")
      .master(master)
      .getOrCreate()

  lazy val executor: LocalVisitor = new LocalVisitor(session)

  private lazy val repl: IntpREPLRunner = new IntpREPLRunner(out)

  def executeCommand(s: String): Results.Result = repl.execute(s)

  def eval(s: String): AnyRef = repl.eval(s)

  def init(): Unit = {
    initRepl()
    initData()
  }

  private def initData(): Unit = {
    // Add data source for demo
    val userDf = spark.read.format("CSV").option("header", "true")
      .schema(StructType(List(
        StructField("userID", IntegerType),
        StructField("age", IntegerType),
        StructField("gender", IntegerType),
        StructField("education", IntegerType),
        StructField("marriageStatus", IntegerType),
        StructField("haveBaby", IntegerType),
        StructField("hometown", IntegerType),
        StructField("residence", IntegerType)
      )))
      // .load("hdfs://10.141.211.91:9000/user/scidb/liangchg/user.csv")
      // .load("file:/C:/user.csv")
      .load(getClass.getClassLoader.getResource("user.csv").toExternalForm)
    userDf.createOrReplaceTempView("user")

    val actionDf = spark.read.format("CSV").option("header", "true")
      .schema(StructType(List(
        StructField("userID", IntegerType),
        StructField("installTime", IntegerType),
        StructField("appID", IntegerType)
      )))
      // .load("hdfs://10.141.211.91:9000/user/scidb/liangchg/user_app_actions.csv")
      // .load("file:/C:/user_app_actions.csv")
      .load(getClass.getClassLoader.getResource("user_app_actions.csv").toExternalForm)
    actionDf.createOrReplaceTempView("action")
  }

  private def initRepl(): Unit = {
    repl.bind("spark", spark)
    repl.bind("sc", spark.sparkContext)
    repl.execute("import org.apache.spark.SparkContext._")
    repl.execute("import spark.implicits._")
    repl.execute("import spark.sql")
    repl.execute("import org.apache.spark.sql.functions._")
    printWelcome()
    // repl.bind("_printStream", new PrintStream(out))
    // repl.execute("System.setOut(_printStream)")
    // repl.execute("Console.setOut(_printStream)")
  }

  /** Print a welcome message */
  def printWelcome() {
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

  def destroy(): Unit = spark.stop()
}