package com.github.mmolimar

import java.util.Properties

import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.io.File
import scala.util.Properties.userHome

package object kukulcan {

  private object Constants {

    private val KUKULCAN_ENV_VAR = "KUKULCAN_HOME"
    private val DEFAULT_KUKULCAN_HOME = s"$userHome/.kukulcan"

    val KUKULCAN_HOME: String = sys.env.get(KUKULCAN_ENV_VAR)
      .orElse(sys.props.get(KUKULCAN_ENV_VAR))
      .getOrElse(DEFAULT_KUKULCAN_HOME)
  }

  private[kukulcan] abstract class Api[K](name: String) {

    final val log: Logger = LoggerFactory.getLogger(getClass)

    log.info(s"Initializing ${name.toUpperCase}...")
    private var _instance: K = initialize()

    private def initialize(): K = {
      val propsFile = s"${Constants.KUKULCAN_HOME}/${name.toLowerCase}.properties"
      val props = new Properties()
      props.load(File(propsFile).inputStream())
      createInstance(props)
    }

    final def reload(): Unit = {
      log.info(s"Reloading ${name.toUpperCase}...")
      _instance = initialize()
    }

    private[kukulcan] def inst: K = _instance

    protected def createInstance(props: Properties): K

  }

  def consumer[K, V]: KConsumer[K, V] = KConsumer.inst.asInstanceOf[KConsumer[K, V]]

  def producer[K, V]: KProducer[K, V] = KProducer.inst.asInstanceOf[KProducer[K, V]]

  def connect: KConnect = KConnect.inst

}
