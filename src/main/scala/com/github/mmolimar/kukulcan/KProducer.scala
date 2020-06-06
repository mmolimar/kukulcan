package com.github.mmolimar.kukulcan

import _root_.java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.tools.{ToolsUtils => JToolsUtils}

import scala.collection.JavaConverters._

private[kukulcan] object KProducer extends Api[KProducer[AnyRef, AnyRef]]("producer") {

  override protected def createInstance(props: Properties): KProducer[AnyRef, AnyRef] = {
    KProducer[AnyRef, AnyRef](props)
  }

}

private[kukulcan] case class KProducer[K, V](props: Properties) extends KafkaProducer[K, V](props) {

  import org.apache.kafka.common.{Metric, MetricName}

  def reload(): Unit = KProducer.reload()

  def getMetrics(groupRegex: String = ".*", nameRegex: String = ".*"): Map[MetricName, Metric] = {
    metrics.asScala
      .filter(metric => metric._1.group.matches(groupRegex) && metric._1.name.matches(nameRegex))
      .toMap
  }

  def listMetrics(groupRegex: String = ".*", nameRegex: String = ".*"): Unit = {
    JToolsUtils.printMetrics(getMetrics(groupRegex, nameRegex).asJava)
  }

}
