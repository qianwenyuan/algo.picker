package fdu.bean.executor

import java.io.OutputStream

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{IMain, JPrintWriter, Results}
/**
  * Created by liangchg on 2017/4/11.
  */

class IntpREPLRunner(val out: OutputStream) {

  protected val settings = new Settings
  settings.processArgumentString("-deprecation -feature -Xfatal-warnings -Xlint -usejavacp")

  private val printWriter: JPrintWriter = new JPrintWriter(out)

  private lazy val repl: IMain =  new IMain(settings, printWriter)

  def eval(s: String): AnyRef = {
    val result = repl.eval(s)
    printWriter.flush()
    result
  }

  def execute(s: String): Results.Result = {
    val result = repl.interpret(s)
    printWriter.flush()
    result
  }

  def bind[T: universe.TypeTag : ClassTag](name: String, value: T): Results.Result = repl.bind(name, value)

  def echo(s: String): Unit = out.write(s.toCharArray.map(_.toByte))
}
