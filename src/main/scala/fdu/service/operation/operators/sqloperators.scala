package fdu.service.operation.operators

import java.util

import breeze.linalg.max
import fdu.bean.generator.OperatorVisitor
import fdu.exceptions.HiveTableNotFoundException
import fdu.service.operation.{BinaryOperation, UnaryOperation}
import fdu.util.UserSession
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, Row, SQLContext}
import org.json.JSONObject
import org.apache.spark.sql
import org.omg.CORBA.UserException

import scala.beans.BeanProperty
import scala.collection.JavaConversions

class DataSource(name: String,
                 `type`: String,
                 @BeanProperty val alias: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {

  @throws(classOf[HiveTableNotFoundException])
  override def execute(user: UserSession): Dataset[Row] = {
    if (!user.getEmbeddedExecutor.tableExists(name))
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

// New model, added by qwy
class GroupBy_Count(name: String,
             `type`: String,
             @BeanProperty val groupbycolumn: String,
                    @BeanProperty val countcolumn: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame = {
    getChild
      .asInstanceOf[CanProduce[Dataset[Row]]]
      .executeCached(session).groupBy(groupbycolumn).count()

  }
  override def toString: String = "([Groupby name: " + name + " column: " + groupbycolumn + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " GROUPBY " + groupbycolumn
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " GROUPBY " + groupbycolumn
      case _ => throw new UnsupportedOperationException("Invalid filter node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: GroupBy_Count =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        groupbycolumn == that.groupbycolumn
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[GroupBy_Count]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, groupbycolumn, countcolumn)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object GroupBy_Count extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new GroupBy_Count(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("groupbycolumn"),
    obj.getString("countcolumn")
  )
}

class Max(name: String,
          `type`: String,
          @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.max(column) as "max_"+column)

  override def toString: String = "([Max name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " MAX(" + column + ")"
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " MAX(" + column + ")"
      case _ => throw new UnsupportedOperationException("Invalid max node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Max =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Max]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Max extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Max(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class GroupBy_Max(name: String,
                    `type`: String,
                    @BeanProperty val groupbycolumn: String,
                    @BeanProperty val maxcolumn: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame = {
    getChild
      .asInstanceOf[CanProduce[Dataset[Row]]]
      .executeCached(session).groupBy(groupbycolumn).agg(org.apache.spark.sql.functions.max(maxcolumn) as "sum_"+maxcolumn+"_groupby_"+groupbycolumn)

  }
  override def toString: String = "([Groupby name: " + name + " column: " + maxcolumn + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " GROUPBY " + maxcolumn
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " GROUPBY " + maxcolumn
      case _ => throw new UnsupportedOperationException("Invalid filter node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: GroupBy_Max =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        groupbycolumn == that.groupbycolumn &&
        maxcolumn == that.maxcolumn
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[GroupBy_Max]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, groupbycolumn, maxcolumn)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object GroupBy_Max extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new GroupBy_Max(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("groupbycolumn"),
    obj.getString("maxcolumn")
  )
}

class Min(name: String,
          `type`: String,
          @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.min(column) as "min_"+column)

  override def toString: String = "([Min name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " MIN(" + column + ")"
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " MIN(" + column + ")"
      case _ => throw new UnsupportedOperationException("Invalid min node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Min =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Min]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Min extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Min(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Sum(name: String,
          `type`: String,
          @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.sum(column) as "sum_"+column)

  override def toString: String = "([Sum name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " SUM(" + column + ")"
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " SUM(" + column + ")"
      case _ => throw new UnsupportedOperationException("Invalid sum node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Sum =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Sum]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Sum extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Sum(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class GroupBy_Sum(name: String,
                  `type`: String,
                  @BeanProperty val groupbycolumn: String,
                  @BeanProperty val sumcolumn: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame = {
    getChild
      .asInstanceOf[CanProduce[Dataset[Row]]]
      .executeCached(session).groupBy(groupbycolumn).agg(org.apache.spark.sql.functions.sum(sumcolumn) as "sum_"+sumcolumn+"_groupby_"+groupbycolumn)

  }
  override def toString: String = "([Groupby name: " + name + " column: " + sumcolumn + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " GROUPBY " + sumcolumn
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " GROUPBY " + sumcolumn
      case _ => throw new UnsupportedOperationException("Invalid filter node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: GroupBy_Sum =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        groupbycolumn == that.groupbycolumn &&
        sumcolumn == that.sumcolumn
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[GroupBy_Max]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, groupbycolumn, sumcolumn)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object GroupBy_Sum extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new GroupBy_Sum(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("groupbycolumn"),
    obj.getString("sumcolumn")
  )
}

class Count(name: String,
          `type`: String,
          @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
    override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.count(column) as "count_" + column)


  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " COUNT " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " COUNT " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid count node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Count =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Count]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Count extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Count(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Count_distinct(name: String,
            `type`: String,
            @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.countDistinct(column) as "count_distinct_"+column)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " COUNT " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " COUNT " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid count node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Count_distinct =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Count_distinct]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Count_distinct extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Count_distinct(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Avg(name: String,
          `type`: String,
          @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.avg(column) as "avg_"+column)

  override def toString: String = "([Avg name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " AVG(" + column + ")"
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " AVG(" + column + ")"
      case _ => throw new UnsupportedOperationException("Invalid sum node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Avg =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Avg]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Avg extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Avg(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

// TopN
class TopN(name: String,
            `type`: String,
            @BeanProperty val column: String,
            @BeanProperty val N: Int)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame = {
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).sort(org.apache.spark.sql.functions.desc(column) as "top_").limit(N)
  }

  override def toString: String = "([TOPN name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = ???

  override def equals(other: Any): Boolean = other match {
    case that: TopN =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column &&
        N == that.N
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[TopN]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column, N)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object TopN extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new TopN(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column"),
    obj.getInt("N")
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

class Asc(name: String,
            `type`: String,
            @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.sort_array(org.apache.spark.sql.functions.asc(column)) as "asc_"+column)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid asc node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Asc =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Count]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Asc extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Asc(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Desc(name: String,
          `type`: String,
          @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.sort_array(org.apache.spark.sql.functions.desc(column)) as "desc_"+column)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid asc node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Desc =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Count]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Desc extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Desc(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Corr(name: String,
           `type`: String,
           @BeanProperty val column1: String,
           @BeanProperty val column2: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.corr(column1, column2) as "corr_"+column1+"&"+column2)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " CORR( " + column1 + ", "+column2+")"
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " CORR( " + column1 + ", "+column2+")"
      case _ => throw new UnsupportedOperationException("Invalid corr node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Corr =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column1 == that.column1 &&
        column2 == that.column2
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Corr]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column1, column2)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Corr extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Corr(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column1"),
    obj.getString("column2")
  )
}

class Covar_pop(name: String,
           `type`: String,
           @BeanProperty val column1: String,
           @BeanProperty val column2: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.covar_pop(column1, column2) as "covar_pop_"+column1+"&"+column2)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " CORR( " + column1 + ", "+column2+")"
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " CORR( " + column1 + ", "+column2+")"
      case _ => throw new UnsupportedOperationException("Invalid corr node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Covar_pop =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column1 == that.column1 &&
        column2 == that.column2
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Covar_pop]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column1, column2)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Covar_pop extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Covar_pop(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column1"),
    obj.getString("column2")
  )
}

class Covar_sample(name: String,
                `type`: String,
                @BeanProperty val column1: String,
                @BeanProperty val column2: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.covar_samp(column1, column2) as "covar_samp_"+column1+"&"+column2)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " CORR( " + column1 + ", "+column2+")"
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " CORR( " + column1 + ", "+column2+")"
      case _ => throw new UnsupportedOperationException("Invalid corr node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Covar_sample =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column1 == that.column1 &&
        column2 == that.column2
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Covar_sample]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column1, column2)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Covar_sample extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Covar_sample(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column1"),
    obj.getString("column2")
  )
}

class Column(name: String,
           `type`: String,
           @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).select(org.apache.spark.sql.functions.column(column) as "column_"+column)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid asc node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Column =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Count]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Column extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Column(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Sin(name: String,
             `type`: String,
             @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.sin(column) as "sin_"+column)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid asc node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Sin =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Count]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Sin extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Sin(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Asin(name: String,
          `type`: String,
          @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.asin(column) as "asin_"+column)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid asc node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Asin =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Count]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Asin extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Asin(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Cos(name: String,
          `type`: String,
          @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.cos(column) as "cos_"+column)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid asc node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Cos =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Count]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Cos extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Cos(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Acos(name: String,
          `type`: String,
          @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.acos(column) as "acos_"+column)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid asc node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Acos =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Count]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Acos extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Acos(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Tan(name: String,
          `type`: String,
          @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.tan(column) as "tan_"+column)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid asc node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Tan =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Count]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Tan extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Tan(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Atan(name: String,
          `type`: String,
          @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.atan(column) as "atan_"+column)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid asc node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Atan =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Count]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Atan extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Atan(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Collect_set(name: String,
                   `type`: String,
                   @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.collect_set(column) as "collect_set_"+column)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid asc node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Collect_set =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Collect_set]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Collect_set extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Collect_set(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Collect_list(name: String,
                  `type`: String,
                  @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.collect_list(column) as "collect_list_"+column)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " LIST " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid asc node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Collect_list =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Collect_list]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Collect_list extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Collect_list(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Kurtosis(name: String,
                   `type`: String,
                   @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.kurtosis(column) as "kurtosis_"+column)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " LIST " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid asc node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Kurtosis =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Kurtosis]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Kurtosis extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Kurtosis(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Skewness(name: String,
               `type`: String,
               @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.skewness(column) as "skewness_"+column)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " LIST " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid asc node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Skewness =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Skewness]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Skewness extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Skewness(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Stddev_pop(name: String,
               `type`: String,
               @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.stddev_pop(column) as "stddev_pop_"+column)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " LIST " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid asc node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Stddev_pop =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Stddev_pop]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Stddev_pop extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Stddev_pop(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Stddev_sample(name: String,
                 `type`: String,
                 @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.stddev_samp(column) as "stddev_samp_"+column)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " LIST " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid asc node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Stddev_sample =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Stddev_sample]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Stddev_sample extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Stddev_sample(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Var_pop(name: String,
                    `type`: String,
                    @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.var_pop(column) as "var_pop_"+column)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " LIST " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid asc node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Var_pop =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Var_pop]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Var_pop extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Var_pop(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Var_sample(name: String,
              `type`: String,
              @BeanProperty val column: String)
  extends UnaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): DataFrame =
    getChild
      .asInstanceOf[CanProduce[DataFrame]]
      .executeCached(session).agg(org.apache.spark.sql.functions.var_samp(column) as "var_samp_"+column)

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {
    val left = getChild.asInstanceOf[SqlOperation]
    left match {
      case _: DataSource => getChild.asInstanceOf[SqlOperation].toSql + " LIST " + column + " "
      case _: Join => getChild.asInstanceOf[SqlOperation].toSql + " ASC " + column + " "
      case _ => throw new UnsupportedOperationException("Invalid asc node")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Var_sample =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type` &&
        column == that.column
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Var_sample]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`, column)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Var_sample extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Var_sample(
    obj.getString("name"),
    obj.getString("type"),
    obj.getString("column")
  )
}

class Union(name: String,
               `type`: String)
  extends BinaryOperation(name, `type`)
    with SqlOperation {
  override def execute(session: UserSession): Dataset[Row] = {
    import org.apache.spark.sql.functions
    getLeft.asInstanceOf[CanProduce[Dataset[Row]]].executeCached(session)
      .union(getRight.asInstanceOf[CanProduce[Dataset[Row]]].executeCached(session))
    }


//  override def execute(session: UserSession): DataFrame =
//    getChild
//      .asInstanceOf[CanProduce[DataFrame]]
//      .executeCached(session).union()
//  /*
//    agg(org.apache.spark.) sqlContext.sql(
//
//                "select a,sum(b) as sum_b from (" +
//                  "select a,b from "+table1+
//                  " union all "+
//                  "select a,b from "+table2+")"+
//                  "tmp group by a"
//                )
//  */

  //override def toString: String = "([Count name: " + name + " column: " + column + "]" + getChild + ")"

  @throws[ClassCastException]
  override def toSql: String = {???}

  override def equals(other: Any): Boolean = other match {
    case that: Union =>
      super.equals(that) &&
        (that canEqual this) &&
        name == that.name &&
        `type` == that.`type`
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Union]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), name, `type`)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def accept(visitor: OperatorVisitor): Unit = ???
}

object Union extends CanGenFromJson {
  def newInstance(obj: JSONObject) = new Union(
    obj.getString("name"),
    obj.getString("type")
  )
}