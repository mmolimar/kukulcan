package com.github.mmolimar.kukulcan

import _root_.com.github.mmolimar.kukulcan.java.{KConsumer => JKConsumer}
import _root_.com.github.mmolimar.kukulcan.{KConsumer => SKConsumer}

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.KafkaConsumer

import _root_.java.util.Properties

class KConsumerSpec extends KukulcanApiTestHarness with EmbeddedKafka {

  lazy implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig()

  override def apiClass: Class[_] = classOf[SKConsumer[String, String]]

  override def execScalaTests(): Unit = withRunningKafka {
    val scalaApi: SKConsumer[String, String] = {
      val props = new Properties()
      props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("group.id", "test-group")
      SKConsumer[String, String](props)
    }
    val kadmin = {
      val props = new Properties()
      props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
      KAdmin(props)
    }
    kadmin.topics.createTopic("test", 1, 1)

    scalaApi.isInstanceOf[KafkaConsumer[String, String]] shouldBe true

    scalaApi.getMetrics shouldBe scalaApi.getMetrics(".*", ".*")
    scalaApi.getMetrics("app-info", "version").head._2.metricValue() shouldBe "2.7.0"

    scalaApi.listMetrics()
    scalaApi.listMetrics("app-info", "version")

    scalaApi.subscribe("test")
  }

  override def execJavaTests(): Unit = withRunningKafka {
    val javaApi: JKConsumer[String, String] = {
      val props = new Properties()
      props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("group.id", "test-group")
      new JKConsumer[String, String](props)
    }
    val kadmin = {
      val props = new Properties()
      props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
      KAdmin(props)
    }
    kadmin.topics.createTopic("test", 1, 1)

    javaApi.isInstanceOf[KafkaConsumer[String, String]] shouldBe true

    javaApi.getMetrics shouldBe javaApi.getMetrics(".*", ".*")
    javaApi.getMetrics("app-info", "version").values().iterator().next().metricValue() shouldBe "2.7.0"

    javaApi.listMetrics()
    javaApi.listMetrics("app-info", "version")

    javaApi.subscribe("test")
  }

}
