package com.github.mmolimar.kukulcan

import _root_.com.github.mmolimar.kukulcan.java.{KProducer => JKProducer}
import _root_.com.github.mmolimar.kukulcan.{KProducer => SKProducer}

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.producer.KafkaProducer

import _root_.java.util.Properties

class KProducerSpec extends KukulcanApiTestHarness with EmbeddedKafka {

  lazy implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig()

  override def apiClass: Class[_] = classOf[SKProducer[String, String]]

  override def execScalaTests(): Unit = withRunningKafka {
    val scalaApi: SKProducer[String, String] = {
      val props = new Properties()
      props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      SKProducer[String, String](props)
    }

    scalaApi.isInstanceOf[KafkaProducer[String, String]] shouldBe true

    scalaApi.getMetrics shouldBe scalaApi.getMetrics(".*", ".*")
    scalaApi.getMetrics("app-info", "version").head._2.metricValue() shouldBe "2.7.0"

    scalaApi.listMetrics()
    scalaApi.listMetrics("app-info", "version")
  }

  override def execJavaTests(): Unit = withRunningKafka {
    val javaApi: JKProducer[String, String] = {
      val props = new Properties()
      props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      new JKProducer[String, String](props)
    }

    javaApi.isInstanceOf[KafkaProducer[String, String]] shouldBe true

    javaApi.getMetrics shouldBe javaApi.getMetrics(".*", ".*")
    javaApi.getMetrics("app-info", "version").values().iterator().next().metricValue() shouldBe "2.7.0"

    javaApi.listMetrics()
    javaApi.listMetrics("app-info", "version")
  }

}
