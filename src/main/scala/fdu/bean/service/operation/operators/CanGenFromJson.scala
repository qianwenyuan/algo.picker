package fdu.bean.service.operation.operators

import fdu.service.operation.Operation
import org.json.JSONObject

/**
  * Created by guoli on 2017/5/13.
  */
trait CanGenFromJson {
  def newInstance(obj: JSONObject): Operation
}
