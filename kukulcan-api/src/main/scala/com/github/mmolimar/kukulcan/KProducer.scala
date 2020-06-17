package com.github.mmolimar.kukulcan

import _root_.java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.tools.{ToolsUtils => JToolsUtils}

import scala.collection.JavaConverters._

/**
 * Factory for [[com.github.mmolimar.kukulcan.KProducer]] instances.
 *
 */
object KProducer {

  def apply[K, V](props: Properties): KProducer[K, V] = new KProducer(props)

}

/**
 * An enriched implementation of the {@code org.apache.kafka.clients.producer.KafkaProducer} class to
 * produce messages in Kafka.
 *
 * @param props Properties with the configuration.
 */
class KProducer[K, V](val props: Properties) extends KafkaProducer[K, V](props) {

  import org.apache.kafka.common.{Metric, MetricName}

  /**
   * Get all metrics registered.
   *
   * @return a {@code Map} with the all metrics registered.
   */
  def getMetrics: Map[MetricName, Metric] = {
    getMetrics(".*", ".*")
  }

  /**
   * Get all metrics registered filtered by the group and name regular expressions.
   *
   * @param groupRegex Regex to filter metrics by group name.
   * @param nameRegex  Regex to filter metrics by its name.
   * @return a {@code Map} with the all metrics registered filtered by the group and name regular expressions.
   */
  def getMetrics(groupRegex: String, nameRegex: String): Map[MetricName, Metric] = {
    metrics.asScala
      .filter(metric => metric._1.group.matches(groupRegex) && metric._1.name.matches(nameRegex))
      .toMap
  }

  /**
   * Print all metrics.
   */
  def listMetrics(): Unit = {
    listMetrics(".*", ".*")
  }

  /**
   * Print all metrics filtered by the group and name regular expressions.
   *
   * @param groupRegex Regex to filter metrics by group name.
   * @param nameRegex  Regex to filter metrics by its name.
   */
  def listMetrics(groupRegex: String, nameRegex: String): Unit = {
    JToolsUtils.printMetrics(getMetrics(groupRegex, nameRegex).asJava)
  }

}
