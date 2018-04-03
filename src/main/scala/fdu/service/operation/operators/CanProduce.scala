package fdu.service.operation.operators

import fdu.exceptions.HiveTableNotFoundException
import fdu.util.UserSession

trait CanProduce[T] {

  @throws(classOf[HiveTableNotFoundException])
  def execute(user: UserSession): T

  @throws(classOf[HiveTableNotFoundException])
  def executeCached(user: UserSession): T = {
    val cache = user.getResultCache
    val original: Option[T] = Option(cache.query(this))
    original match {
      case None =>
        val result = execute(user)
        cache.commit(this, result)
        result
      case Some(res: T) => res
    }
  }
}
