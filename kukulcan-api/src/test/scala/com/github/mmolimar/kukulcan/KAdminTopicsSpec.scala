package com.github.mmolimar.kukulcan

import _root_.com.github.mmolimar.kukulcan.{KAdmin => SKAdmin}

import com.github.mmolimar.kukulcan.java.{KUtils, KAdmin => JKAdmin}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.admin.{CreateTopicsOptions, KafkaAdminClient, NewTopic}

import _root_.java.time.Duration
import _root_.java.util.{Properties, Arrays => JArrays, HashSet => JSet}

class KAdminTopicsSpec extends KukulcanApiTestHarness with EmbeddedKafka {

  lazy implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig()

  override def apiClass: Class[_] = classOf[SKAdmin]

  override def execScalaTests(): Unit = withRunningKafka {
    val scalaApi: SKAdmin = {
      val props = new Properties()
      props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
      SKAdmin(props)
    }
    val kconsumer = {
      val props = new Properties()
      props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("group.id", "test-group")
      KConsumer[String, String](props)
    }

    scalaApi.servers shouldBe s"localhost:${config.kafkaPort}"
    scalaApi.client.isInstanceOf[KafkaAdminClient] shouldBe true

    scalaApi.topics.getTopics(excludeInternalTopics = true).size shouldBe 0
    scalaApi.topics.listTopics()

    scalaApi.topics.createTopic("topic1", 1, 1) shouldBe true
    scalaApi.topics.createTopicWithReplicasAssignments("topic2", Map(0 -> Seq(0))) shouldBe true
    scalaApi.topics.createTopics(Seq(new NewTopic("topic1", 1, 1.toShort))) shouldBe true
    scalaApi.topics.createTopics(Seq(new NewTopic("topic3", 1, 1.toShort))) shouldBe true

    kconsumer.subscribe("topic1")
    kconsumer.poll(Duration.ofMillis(2000))

    scalaApi.topics.getTopics(Some("topic1")) shouldBe Seq("topic1")
    scalaApi.topics.getTopics(None).toSet shouldBe
      Seq("topic1", "topic2", "topic3", "__consumer_offsets").toSet
    scalaApi.topics.getTopics(None, excludeInternalTopics = true) shouldBe Seq("topic1", "topic2", "topic3")
    scalaApi.topics.listTopics()

    scalaApi.topics.getTopicAndPartitionDescription("topic1", None).get._1.topic shouldBe "topic1"
    scalaApi.topics.getTopicAndPartitionDescription(Seq("topic1")).head._1.topic shouldBe "topic1"

    scalaApi.topics.describeTopic("topic1")
    scalaApi.topics.describeTopics(Seq("topic1"))

    scalaApi.topics.getNewPartitionsDistribution("topic1", 1, Map(0 -> Seq(0)))
      .head._2.totalCount() shouldBe 1
    scalaApi.topics.getNewPartitionsDistribution(Seq("topic1"), 2, Map(0 -> Seq(0)))
      .head._2.totalCount() shouldBe 2

    scalaApi.topics.alterTopic("topic1", 2, Map(0 -> Seq(0), 1 -> Seq(0)))
    scalaApi.topics.alterTopics(Seq("topic2"), 2, Map(0 -> Seq(0), 1 -> Seq(0)))

    scalaApi.topics.deleteTopic("topic1")
    scalaApi.topics.deleteTopic("non-existent")
    scalaApi.topics.deleteTopics(Seq("topic2", "topic3"))
    scalaApi.topics.getTopics(None, excludeInternalTopics = true).size shouldBe 0
  }

  override def execJavaTests(): Unit = withRunningKafka {
    val javaApi: JKAdmin = {
      val props = new Properties()
      props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
      new JKAdmin(props)
    }
    val kconsumer = {
      val props = new Properties()
      props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("group.id", "test-group")
      KConsumer[String, String](props)
    }

    javaApi.servers shouldBe s"localhost:${config.kafkaPort}"
    javaApi.client.isInstanceOf[KafkaAdminClient] shouldBe true

    javaApi.topics().getTopics(true).size shouldBe 0
    javaApi.topics().listTopics()

    javaApi.topics().createTopic("topic1", 1, 1) shouldBe true
    val replicaAssignments = KUtils.toJavaMap(Map(Integer.valueOf(0) -> JArrays.asList(Integer.valueOf(0))))
    javaApi.topics() createTopicWithReplicasAssignments("topic2", replicaAssignments) shouldBe true
    javaApi.topics().createTopics(
      JArrays.asList(new NewTopic("topic1", 1, 1.toShort))) shouldBe true
    javaApi.topics().createTopics(
      JArrays.asList(new NewTopic("topic3", 1, 1.toShort)), new CreateTopicsOptions) shouldBe true

    kconsumer.subscribe("topic1")
    kconsumer.poll(Duration.ofMillis(2000))

    javaApi.topics().getTopics("topic1", false) shouldBe JArrays.asList("topic1")
    new JSet[String](javaApi.topics().getTopics(false)) shouldBe
      new JSet[String](JArrays.asList("topic1", "topic2", "topic3", "__consumer_offsets"))
    new JSet[String](javaApi.topics().getTopics(true)) shouldBe
      new JSet[String](JArrays.asList("topic1", "topic2", "topic3"))
    javaApi.topics().listTopics(false)

    javaApi.topics().describeTopic("topic1")
    javaApi.topics().describeTopics(JArrays.asList("topic1"))

    var replicaAssignment = KUtils.toJavaMap(Map(Integer.valueOf(0) -> JArrays.asList(Integer.valueOf(0))))
    javaApi.topics().getNewPartitionsDistribution("topic1", 1, replicaAssignment)
      .get("topic1").totalCount() shouldBe 1
    javaApi.topics().getNewPartitionsDistribution(JArrays.asList("topic1"), 2, replicaAssignment)
      .get("topic1").totalCount() shouldBe 2

    replicaAssignment = KUtils.toJavaMap(Map(
      Integer.valueOf(0) -> JArrays.asList(Integer.valueOf(0)),
      Integer.valueOf(1) -> JArrays.asList(Integer.valueOf(0)),
    ))
    javaApi.topics().alterTopic("topic1", 2, replicaAssignment)
    javaApi.topics().alterTopics(JArrays.asList("topic2"), 2, replicaAssignment)

    javaApi.topics().deleteTopic("topic1")
    javaApi.topics().deleteTopic("non-existent")
    javaApi.topics().deleteTopics(JArrays.asList("topic2", "topic3"))
    javaApi.topics().getTopics(true).size shouldBe 0
  }

}
