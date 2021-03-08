package com.github.mmolimar.kukulcan

import _root_.com.github.mmolimar.kukulcan.{KAdmin => SKAdmin}

import com.github.mmolimar.kukulcan.java.{KAdmin => JKAdmin}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.common.errors.InvalidConfigurationException

import _root_.java.util.Collections._
import _root_.java.util.Properties

class KAdminConfigsSpec extends KukulcanApiTestHarness with EmbeddedKafka {

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

    scalaApi.topics.getTopics(excludeInternalTopics = true).size shouldBe 0
    scalaApi.topics.createTopic("test", 1, 1)
    scalaApi.topics.getTopics(excludeInternalTopics = true).size shouldBe 1

    scalaApi.configs.getDescribeBrokerConfig()
      .find(config => config.name() == "broker.id" && config.value() == "0").size shouldBe 1
    scalaApi.configs.getDescribeBrokerConfig(Some(0))
      .find(config => config.name() == "broker.id" && config.value() == "0").size shouldBe 1
    scalaApi.configs.getDescribeBrokerConfig(Some(0)).find(config => config.name() == "test.config").size shouldBe 0
    scalaApi.configs.alterBrokerConfig(None, Map("test.config" -> "test"), Seq.empty)
    assertThrows[InvalidConfigurationException] {
      scalaApi.configs.alterBrokerConfig(Some(0), Map("sensitive.config" -> None.orNull), Seq.empty)
    }
    Thread.sleep(1000)
    scalaApi.configs.getDescribeBrokerConfig(Some(0))
      .find(config => config.name() == "test.config" && config.value() == None.orNull).size shouldBe 1
    scalaApi.configs.alterBrokerConfig(None, Map.empty, Seq("test.config"))
    Thread.sleep(1000)
    scalaApi.configs.getDescribeBrokerConfig(Some(0)).find(config => config.name() == "test.config").size shouldBe 0
    scalaApi.configs.alterBrokerConfig(Some(0), Map("log.flush.interval.ms" -> "999"), Seq.empty)
    Thread.sleep(1000)
    scalaApi.configs.getDescribeBrokerConfig(Some(0))
      .find(config => config.name() == "log.flush.interval.ms" && config.value() == "999").size shouldBe 1
    scalaApi.configs.describeBrokerConfig()
    scalaApi.configs.describeBrokerConfig(Some(0))

    scalaApi.configs.alterBrokerLoggerConfig(0, Map("org.apache.zookeeper.CreateMode" -> "TRACE"), Seq.empty)
    Thread.sleep(1000)
    scalaApi.configs.getDescribeBrokerLoggerConfig()
      .find(config => config.name() == "org.apache.zookeeper.CreateMode" && config.value() == "TRACE").size shouldBe 1
    scalaApi.configs.getDescribeBrokerLoggerConfig(Some(0))
      .find(config => config.name() == "org.apache.zookeeper.CreateMode" && config.value() == "TRACE").size shouldBe 1
    assertThrows[InvalidConfigurationException] {
      scalaApi.configs.alterBrokerLoggerConfig(0, Map("test.config" -> "test"), Seq.empty)
    }
    scalaApi.configs.describeBrokerLoggerConfig()
    scalaApi.configs.describeBrokerLoggerConfig(Some(0))

    scalaApi.configs.getDescribeTopicConfig()
      .find(config => config.name() == "cleanup.policy" && config.value() == "delete").size shouldBe 1
    scalaApi.configs.getDescribeTopicConfig(Some("test"))
      .find(config => config.name() == "cleanup.policy" && config.value() == "delete").size shouldBe 1
    scalaApi.configs.alterTopicConfig("test", Map("cleanup.policy" -> "compact"), Seq.empty)
    Thread.sleep(1000)
    scalaApi.configs.getDescribeTopicConfig(Some("test"))
      .find(config => config.name() == "cleanup.policy" && config.value() == "compact").size shouldBe 1
    scalaApi.configs.describeTopicConfig()
    scalaApi.configs.describeTopicConfig(Some("test"))
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

    javaApi.topics.getTopics(true).size shouldBe 0
    javaApi.topics.createTopic("test", 1, 1)
    javaApi.topics.getTopics(true).size shouldBe 1

    javaApi.configs.getDescribeBrokerConfig().asScala
      .find(config => config.name() == "broker.id" && config.value() == "0").size shouldBe 1
    javaApi.configs.getDescribeBrokerConfig(0).asScala
      .find(config => config.name() == "broker.id" && config.value() == "0").size shouldBe 1
    javaApi.configs.getDescribeBrokerConfig(0).asScala
      .find(config => config.name() == "test.config").size shouldBe 0
    javaApi.configs.alterBrokerConfig(singletonMap("test.config", "test"), emptyList())
    assertThrows[InvalidConfigurationException] {
      javaApi.configs.alterBrokerConfig(0, singletonMap("sensitive.config", None.orNull), emptyList())
    }
    Thread.sleep(1000)
    javaApi.configs.getDescribeBrokerConfig(0).asScala
      .find(config => config.name() == "test.config" && config.value() == None.orNull).size shouldBe 1
    javaApi.configs.alterBrokerConfig(emptyMap(), singletonList("test.config"))
    Thread.sleep(1000)
    javaApi.configs.getDescribeBrokerConfig(0).asScala
      .find(config => config.name() == "test.config").size shouldBe 0
    javaApi.configs.alterBrokerConfig(0, singletonMap("log.flush.interval.ms", "999"), emptyList())
    Thread.sleep(1000)
    javaApi.configs.getDescribeBrokerConfig(0).asScala
      .find(config => config.name() == "log.flush.interval.ms" && config.value() == "999").size shouldBe 1
    javaApi.configs.describeBrokerConfig()
    javaApi.configs.describeBrokerConfig(0)

    javaApi.configs.alterBrokerLoggerConfig(0, singletonMap("org.apache.zookeeper.CreateMode", "TRACE"), emptyList())
    Thread.sleep(1000)
    javaApi.configs.getDescribeBrokerLoggerConfig().asScala
      .find(config => config.name() == "org.apache.zookeeper.CreateMode" && config.value() == "TRACE").size shouldBe 1
    javaApi.configs.getDescribeBrokerLoggerConfig(0).asScala
      .find(config => config.name() == "org.apache.zookeeper.CreateMode" && config.value() == "TRACE").size shouldBe 1
    assertThrows[InvalidConfigurationException] {
      javaApi.configs.alterBrokerLoggerConfig(0, singletonMap("test.config", "test"), emptyList())
    }
    javaApi.configs.getDescribeBrokerLoggerConfig(0).asScala
      .find(config => config.name() == "org.apache.zookeeper.CreateMode" && config.value() == "TRACE").size shouldBe 1
    javaApi.configs.alterBrokerLoggerConfig(0, singletonMap("org.apache.zookeeper.CreateMode", "TRACE"), emptyList())
    javaApi.configs.describeBrokerLoggerConfig()
    javaApi.configs.describeBrokerLoggerConfig(0)

    javaApi.configs.getDescribeTopicConfig().asScala
      .find(config => config.name() == "cleanup.policy" && config.value() == "delete").size shouldBe 1
    javaApi.configs.getDescribeTopicConfig("test").asScala
      .find(config => config.name() == "cleanup.policy" && config.value() == "delete").size shouldBe 1
    javaApi.configs.alterTopicConfig("test", singletonMap("cleanup.policy", "compact"), emptyList())
    Thread.sleep(1000)
    javaApi.configs.getDescribeTopicConfig("test").asScala
      .find(config => config.name() == "cleanup.policy" && config.value() == "compact").size shouldBe 1
    javaApi.configs.describeTopicConfig()
    javaApi.configs.describeTopicConfig("test")
  }

}
