package com.github.mmolimar.kukulcan.repl

import _root_.java.util.Properties

import scala.reflect.io.File
import scala.util.Properties.userDir

class KukulcanReplException(message: String = None.orNull, cause: Throwable = None.orNull)
  extends RuntimeException(message, cause)

private[kukulcan] abstract class KApi[K <: AnyRef](name: String) {

  private val KUKULCAN_ENV_VAR = "KUKULCAN_HOME"

  val KUKULCAN_HOME: String = sys.env.get(KUKULCAN_ENV_VAR)
    .orElse(sys.props.get(KUKULCAN_ENV_VAR))
    .getOrElse(userDir)


  private var _instance: K = initialize()

  private def initialize(): K = {
    val propsFile = File(s"$KUKULCAN_HOME${File.separator}config${File.separator}${name.trim.toLowerCase}.properties")
    if (!propsFile.exists) {
      throw new KukulcanReplException(s"Cannot locate config file '${propsFile.jfile.getAbsolutePath}'. " +
        s"Do you have properly set '$KUKULCAN_ENV_VAR' environment variable?")
    }
    val props = new Properties()
    props.load(propsFile.inputStream())
    createInstance(props)
  }

  final def reload(): Unit = _instance = initialize()

  private[kukulcan] def inst: K = _instance

  protected def createInstance(props: Properties): K

}
