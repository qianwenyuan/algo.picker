package fdu.service.operation.operators

import org.apache.spark.sql.DataFrame

trait SqlOperation extends CanProduce[DataFrame] {
  def toSql: String
}
