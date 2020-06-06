package com.github.mmolimar.kukulcan

import _root_.java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.tools.{ToolsUtils => JToolsUtils}

import scala.collection.JavaConverters._

private[kukulcan] object KConsumer extends Api[KConsumer[AnyRef, AnyRef]]("consumer") {

  override protected def createInstance(props: Properties): KConsumer[AnyRef, AnyRef] = {
    new KConsumer[AnyRef, AnyRef](props)
  }

}

class KConsumer[K, V](val props: Properties) extends KafkaConsumer[K, V](props) {

  import org.apache.kafka.common.{Metric, MetricName}

  def reload(): Unit = KConsumer.reload()

  def subscribe(topic: String): Unit = subscribe(Seq(topic))

  def subscribe(topics: Seq[String]): Unit = subscribe(topics.asJava)

  def getMetrics(groupRegex: String = ".*", nameRegex: String = ".*"): Map[MetricName, Metric] = {
    metrics.asScala
      .filter(metric => metric._1.group.matches(groupRegex) && metric._1.name.matches(nameRegex))
      .toMap
  }

  def listMetrics(groupRegex: String = ".*", nameRegex: String = ".*"): Unit = {
    JToolsUtils.printMetrics(getMetrics(groupRegex, nameRegex).asJava)
  }

}
