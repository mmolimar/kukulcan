package com.github.mmolimar.kukulcan

import _root_.com.github.mmolimar.kukulcan.{KAdmin => SKAdmin}

import com.github.mmolimar.kukulcan.java.{KAdmin => JKAdmin}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.admin.KafkaAdminClient

import _root_.java.util.Properties

class KAdminMetricsSpec extends KukulcanApiTestHarness with EmbeddedKafka {

  lazy implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig()

  override def apiClass: Class[_] = classOf[SKAdmin]

  override def execScalaTests(): Unit = withRunningKafka {
    val scalaApi: SKAdmin = {
      val props = new Properties()
      props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
      SKAdmin(props)
    }

    scalaApi.servers shouldBe s"localhost:${config.kafkaPort}"
    scalaApi.client.isInstanceOf[KafkaAdminClient] shouldBe true

    scalaApi.metrics.getMetrics shouldBe scalaApi.metrics.getMetrics(".*", ".*")
    scalaApi.metrics.getMetrics("app-info", "version").head._2.metricValue() shouldBe "2.7.0"

    scalaApi.metrics.listMetrics()
    scalaApi.metrics.listMetrics("app-info", "version")
  }

  override def execJavaTests(): Unit = withRunningKafka {
    import scala.collection.JavaConverters._

    val javaApi: JKAdmin = {
      val props = new Properties()
      props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
      new JKAdmin(props)
    }

    javaApi.servers shouldBe s"localhost:${config.kafkaPort}"
    javaApi.client.isInstanceOf[KafkaAdminClient] shouldBe true

    javaApi.metrics.getMetrics shouldBe javaApi.metrics.getMetrics(".*", ".*")
    javaApi.metrics.getMetrics("app-info", "version").asScala.head._2.metricValue() shouldBe "2.7.0"

    javaApi.metrics.listMetrics()
    javaApi.metrics.listMetrics("app-info", "version")
  }

}
