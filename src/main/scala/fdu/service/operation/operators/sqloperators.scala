package fdu.service.operation.operators

import java.util

import fdu.bean.generator.OperatorVisitor
import fdu.exceptions.HiveTableNotFoundException
import fdu.service.operation.{BinaryOperation, UnaryOperation}
import fdu.util.UserSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.{Dataset, Row}
import org.json.JSONObject

import scala.beans.BeanProperty
import scala.collection.JavaConversions

class DataSource(name: String,
                 `type`: String,
                 @BeanProperty val alias: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {

  @throws(classOf[HiveTableNotFoundException])
  override def execute(user: UserSession): Dataset[Row] = {
    if (user.getEmbeddedExecutor.tableExists(name))
      throw new HiveTableNotFoundException()
    else user.getSparkSession.table(name).as(alias)
  }

  override def accept(visitor: OperatorVisitor): Unit = {
    visitor.visitDataSource(this)
  }

  override def toString: String = "([Datasource]: " + name + " as " + alias + ")"

  override def toSql: String = name + ((if (alias == null || alias.length == 0) ""
  else " AS " + alias) + " ")

  override def equals(other: Any): Boolean = other match {
    case that: DataSource =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        alias == that.alias
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[DataSource]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, alias)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object DataSource extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new DataSource(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("alias")
  )
}


class Filter(name: String,
             `type`: String,
             @BeanProperty val condition: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): Dataset[Row] =
    getChild
      .asInstanceOf[CanProduce[Dataset[Row]]]
      .executeCached(session).filter(condition)

  override def accept(visitor: OperatorVisitor): Unit = {
    getChild.accept(visitor)
    visitor.visitFilter(this)
  }

  override def toString: String = "([Filter name: " + name + " condition: " + condition + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " WHERE " + condition
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " WHERE " + condition
      case _ => throw new UnsupportedOperationException("Invalid filter node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Filter =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        condition == that.condition
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Filter]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, condition)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object Filter extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Filter(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("condition")
  )
}

class Join(name: String,
           `type`: String,
           @BeanProperty val condition: String,
           @BeanProperty val useExpression: String)
  extends BinaryOperation(name, `type`)
    with SqlOperation {
  override def accept(visitor: OperatorVisitor): Unit = {
    getLeft.accept(visitor)
    getRight.accept(visitor)
    visitor.visitJoin(this)
  }

  override def toString: String = "([Join : " + condition + "] " + getLeft + " " + getRight + ")"

  @throws[ClassCastException]
  override def toSql: String = getLeft.asInstanceOf[SqlOperation].toSql + " JOIN " + getRight.asInstanceOf[SqlOperation].toSql + " ON " + getCondition + " "

  override def execute(session: UserSession): Dataset[Row] = {
    import org.apache.spark.sql.functions
    if (useExpression.trim.toLowerCase == "true")
      getLeft.asInstanceOf[CanProduce[Dataset[Row]]].executeCached(session)
        .join(getRight.asInstanceOf[CanProduce[Dataset[Row]]].executeCached(session), functions.expr(condition))
    else getLeft.asInstanceOf[CanProduce[Dataset[Row]]].executeCached(session)
      .join(getRight.asInstanceOf[CanProduce[Dataset[Row]]].executeCached(session), JavaConversions.asScalaIterator(getJoinFieldList.iterator).toSeq)
  }

  private def getJoinFieldList: util.List[String] = {
    val result: util.List[String] = new util.ArrayList[String]
    for (s <- condition.split(",")) {
      result.add(s.trim)
    }
    result
  }

  override def equals(other: Any): Boolean = other match {
    case that: Join =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        condition == that.condition &&
        useExpression == that.useExpression
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Join]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, condition, useExpression)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object Join extends CanGenFromJson {
  def newInstance(obj: JSONObject): Join = new Join(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("condition"),
    obj.getString("useExpression")
  )
}


class Project(name: String,
              `type`: String,
              @BeanProperty val projections: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def accept(visitor: OperatorVisitor): Unit = {
    getChild.accept(visitor)
    visitor.visitProject(this)
  }

  def getFormattedProjections: String = {
    if ("*" == projections) throw new AssertionError("* is not supported yet, you should specify all column name")
    val cols = projections.split(",")
    val result = new StringBuilder(projections.length)
    for (col <- cols) {
      result.append("\"")
      result.append(col.trim)
      result.append("\"")
      result.append(",")
    }
    result.substring(0, result.length - 1)
  }

  override def toString: String = "([Project: " + projections + "] " + getChild + ")"

  override def toSql: String = "SELECT " + projections + " FROM " + getChild.asInstanceOf[SqlOperation].toSql

  override def execute(session: UserSession): Dataset[Row] = {
    val child = getChild.asInstanceOf[CanProduce[Dataset[Row]]].executeCached(session)
    if (isProjectAll) child
    else {
      val cols = getProjectionList.toArray.asInstanceOf[Array[String]]
      child.select(cols.head, cols.tail: _*)
    }
  }

  private def getProjectionList = {
    projections.split(",").map(_.trim)
    val result = new util.ArrayList[String]
    for (s <- projections.split(",")) {
      result.add(s.trim)
    }
    result
  }

  private def isProjectAll = projections.trim == "*"

  override def equals(other: Any): Boolean = other match {
    case that: Project =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        projections == that.projections
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Project]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, projections)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object Project extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Project(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("projections")
  )
}