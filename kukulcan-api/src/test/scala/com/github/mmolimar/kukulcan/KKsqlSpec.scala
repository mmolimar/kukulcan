package com.github.mmolimar.kukulcan

import _root_.com.github.mmolimar.kukulcan.java.{KKsql => JKKsql}
import _root_.com.github.mmolimar.kukulcan.{KKsql => SKKsql}
import io.confluent.ksql.rest.entity.{CommandStatus, CommandStatusEntity}
import io.confluent.ksql.rest.server.{KsqlRestApplication, KsqlRestConfig}
import io.confluent.ksql.util.KsqlException
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}

import _root_.java.util.Properties
import _root_.java.util.Collections

class KKsqlSpec extends KukulcanApiTestHarness with EmbeddedKafka {

  lazy implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig()

  val KSQL_PORT = 37003
  val ksqlListener = s"http://localhost:$KSQL_PORT"

  override def apiClass: Class[_] = classOf[SKKsql]

  override def execScalaTests(): Unit = withRunningKafka {
    withKsqlServer {
      val scalaApi: SKKsql = {
        val props = new Properties()
        props.put("ksql.server", ksqlListener)
        props.put("ksql.credentials.user", "testuser")
        props.put("ksql.credentials.password", "testpassword")
        SKKsql(props)
      }
      scalaApi.getServerAddress.toString shouldBe ksqlListener
      scalaApi.getServerInfo.getServerStatus shouldBe "RUNNING"
      scalaApi.getServerMetadata.getVersion shouldBe "6.1.0"
      scalaApi.getServerMetadataId.getId shouldBe ""
      scalaApi.getServerHealth.getIsHealthy shouldBe true
      scalaApi.getAllStatuses.size() shouldBe 0

      scalaApi.setProperty("ksql.streams.auto.offset.reset", "earliest") shouldBe None.orNull
      scalaApi.unsetProperty("ksql.streams.auto.offset.reset").toString shouldBe "earliest"

      val ksqlCreate = "CREATE TABLE TEST (ID VARCHAR PRIMARY KEY) WITH (KAFKA_TOPIC = 'test', VALUE_FORMAT = 'DELIMITED');"
      val createResult = scalaApi.makeKsqlRequest(ksqlCreate, -1L)
      scalaApi.getStatus(createResult.get(0).asInstanceOf[CommandStatusEntity].getCommandId.toString).getStatus shouldBe
        CommandStatus.Status.SUCCESS

      val ksqlInsert = "INSERT INTO TEST (ID) VALUES ('test');"
      scalaApi.makeKsqlRequest(ksqlInsert).size() shouldBe 0
      scalaApi.getAllStatuses.size() shouldBe 1

      val ksqlQuery = "SELECT * FROM TEST EMIT CHANGES LIMIT 0;"
      scalaApi.makeQueryRequest(ksqlQuery, -1L).size shouldBe 2
      scalaApi.makeQueryRequestStreamed(ksqlQuery, -1L) shouldNot be(None.orNull)

      val ksqlPrint = "PRINT test;"
      scalaApi.makePrintTopicRequest(ksqlPrint, -1L) shouldNot be(None.orNull)

      assertThrows[KsqlException] {
        scalaApi.makeHeartbeatRequest
      }
      assertThrows[KsqlException] {
        scalaApi.makeClusterStatusRequest
      }
    }
  }

  override def execJavaTests(): Unit = withRunningKafka {
    withKsqlServer {
      val javaApi: JKKsql = {
        val props = new Properties()
        props.put("ksql.server", ksqlListener)
        new JKKsql(props)
      }

      javaApi.getServerAddress.toString shouldBe ksqlListener
      javaApi.getServerInfo.getServerStatus shouldBe "RUNNING"
      javaApi.getServerMetadata.getVersion shouldBe "6.1.0"
      javaApi.getServerMetadataId.getId shouldBe ""
      javaApi.getServerHealth.getIsHealthy shouldBe true
      javaApi.getAllStatuses.size() shouldBe 0

      javaApi.setProperty("ksql.streams.auto.offset.reset", "earliest") shouldBe None.orNull
      javaApi.unsetProperty("ksql.streams.auto.offset.reset").toString shouldBe "earliest"

      val ksqlCreate = "CREATE TABLE TEST (ID VARCHAR PRIMARY KEY) WITH (KAFKA_TOPIC = 'test', VALUE_FORMAT = 'DELIMITED');"
      val createResult = javaApi.makeKsqlRequest(ksqlCreate, -1L)
      javaApi.getStatus(createResult.get(0).asInstanceOf[CommandStatusEntity].getCommandId.toString).getStatus shouldBe
        CommandStatus.Status.SUCCESS

      val ksqlInsert = "INSERT INTO TEST (ID) VALUES ('test');"
      javaApi.makeKsqlRequest(ksqlInsert).size() shouldBe 0
      javaApi.getAllStatuses.size() shouldBe 1

      val ksqlQuery = "SELECT * FROM TEST EMIT CHANGES LIMIT 0;"
      javaApi.makeQueryRequest(ksqlQuery, -1L).size shouldBe 2
      javaApi.makeQueryRequest(ksqlQuery, -1L, Collections.emptyMap(), Collections.emptyMap()).size shouldBe 2
      javaApi.makeQueryRequestStreamed(ksqlQuery, -1L) shouldNot be(None.orNull)

      val ksqlPrint = "PRINT test;"
      javaApi.makePrintTopicRequest(ksqlPrint, -1L) shouldNot be(None.orNull)

      assertThrows[KsqlException] {
        javaApi.makeHeartbeatRequest
      }
      assertThrows[KsqlException] {
        javaApi.makeClusterStatusRequest
      }
    }
  }

  private def withKsqlServer(body: => Unit): Unit = {
    val ksqlServer = {
      val props = new Properties()
      props.put("listeners", ksqlListener)
      props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
      val ksqlRestConfig = new KsqlRestConfig(props)
      KsqlRestApplication.buildApplication(ksqlRestConfig)
    }
    ksqlServer.startAsync()

    val kadmin = {
      val props = new Properties()
      props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
      KAdmin(props)
    }
    kadmin.topics.createTopic("test", 1, 1)

    body

    ksqlServer.notifyTerminated()
    ksqlServer.shutdown()
  }

}
