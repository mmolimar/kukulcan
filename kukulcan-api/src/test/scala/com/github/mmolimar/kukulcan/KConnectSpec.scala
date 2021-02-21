package com.github.mmolimar.kukulcan

import _root_.com.github.mmolimar.kukulcan.java.{KConnect => JKConnect}
import _root_.com.github.mmolimar.kukulcan.{KConnect => SKConnect}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy
import org.apache.kafka.connect.file.FileStreamSourceConnector
import org.apache.kafka.connect.runtime.isolation.Plugins
import org.apache.kafka.connect.runtime.rest.RestServer
import org.apache.kafka.connect.runtime.standalone.{StandaloneConfig, StandaloneHerder}
import org.apache.kafka.connect.runtime.{Connect, Worker, WorkerConfig}
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore
import org.apache.kafka.connect.util.ConnectUtils

import _root_.java.util.Properties
import scala.reflect.io.File

class KConnectSpec extends KukulcanApiTestHarness with EmbeddedKafka {

  lazy implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig()

  val CONNECT_PORT = 37003
  val connectListener = s"http://localhost:$CONNECT_PORT"

  override def apiClass: Class[_] = classOf[SKConnect]

  override def execScalaTests(): Unit = withRunningKafka {
    withConnect {
      val scalaApi: SKConnect = {
        val props = new Properties()
        props.put("rest.host.name", "localhost")
        props.put("rest.port", s"$CONNECT_PORT")
        SKConnect(props)
      }
      val connectorConfig = Map(
        "name" -> "test-connector",
        "connector.class" -> classOf[FileStreamSourceConnector].getName,
        "topic" -> "test",
        "file" -> "sample.txt"
      )

      scalaApi.serverVersion.version shouldBe "2.7.0"
      scalaApi.serverVersion.toJson
      scalaApi.serverVersion.printJson()
      scalaApi.addConnector("test-connector", connectorConfig).name shouldBe "test-connector"
      scalaApi.validateConnectorPluginConfig(classOf[FileStreamSourceConnector].getName, connectorConfig)
        .name shouldBe classOf[FileStreamSourceConnector].getName
      scalaApi.connector("test-connector").name shouldBe "test-connector"
      scalaApi.connectorTopics("test-connector").name shouldBe "test-connector"

      scalaApi.connectorConfig("test-connector") shouldBe connectorConfig
      scalaApi.connectorStatus("test-connector").tasks.size shouldBe 1

      val taskId = scalaApi.connectorTasks("test-connector").head.id.task
      scalaApi.connectorTaskStatus("test-connector", taskId).state shouldBe "RUNNING"
      scalaApi.connectorPlugins shouldNot be(None.orNull)

      scalaApi.updateConnector("test-connector", connectorConfig ++ Map("file" -> "sample2.txt"))
        .name shouldBe "test-connector"
      scalaApi.resetConnectorTopics("test-connector") shouldBe true

      scalaApi.connectors.size shouldBe 1
      scalaApi.connectorsWithExpandedInfo.connectorNames.size shouldBe 1
      scalaApi.connectorsWithExpandedStatus.statuses.size shouldBe 1
      scalaApi.connectorsWithAllExpandedMetadata.definitions.size shouldBe 1

      scalaApi.pauseConnector("test-connector") shouldBe true
      scalaApi.resumeConnector("test-connector")

      scalaApi.restartConnector("test-connector") shouldBe true
      scalaApi.restartConnectorTask("test-connector", taskId) shouldBe true

      scalaApi.deleteConnector("test-connector") shouldBe true
    }
  }

  override def execJavaTests(): Unit = withRunningKafka {
    import scala.collection.JavaConverters._

    withConnect {
      val javaApi: JKConnect = {
        val props = new Properties()
        props.put("rest.host.name", "localhost")
        props.put("rest.port", s"$CONNECT_PORT")
        new JKConnect(props)
      }

      val connectorConfig = Map(
        "name" -> "test-connector",
        "connector.class" -> classOf[FileStreamSourceConnector].getName,
        "topic" -> "test",
        "file" -> "sample.txt"
      )

      javaApi.serverVersion.version shouldBe "2.7.0"
      javaApi.serverVersion.toJson
      javaApi.serverVersion.printJson()
      javaApi.addConnector("test-connector", connectorConfig.asJava).name shouldBe "test-connector"
      javaApi.validateConnectorPluginConfig(classOf[FileStreamSourceConnector].getName, connectorConfig.asJava)
        .name shouldBe classOf[FileStreamSourceConnector].getName
      javaApi.connector("test-connector").name shouldBe "test-connector"
      javaApi.connectorTopics("test-connector").name shouldBe "test-connector"

      javaApi.connectorConfig("test-connector") shouldBe connectorConfig.asJava
      javaApi.connectorStatus("test-connector").tasks.size shouldBe 1

      val taskId = javaApi.connectorTasks("test-connector").get(0).id.task
      javaApi.connectorTaskStatus("test-connector", taskId).state shouldBe "RUNNING"
      javaApi.connectorPlugins shouldNot be(None.orNull)

      javaApi.updateConnector("test-connector", (connectorConfig ++ Map("file" -> "sample2.txt")).asJava)
        .name shouldBe "test-connector"
      javaApi.resetConnectorTopics("test-connector") shouldBe true

      javaApi.connectors.size shouldBe 1
      javaApi.connectorsWithExpandedInfo.connectorNames.size shouldBe 1
      javaApi.connectorsWithExpandedStatus.statuses.size shouldBe 1
      javaApi.connectorsWithAllExpandedMetadata.definitions.size shouldBe 1

      javaApi.pauseConnector("test-connector") shouldBe true
      javaApi.resumeConnector("test-connector")

      javaApi.restartConnector("test-connector") shouldBe true
      javaApi.restartConnectorTask("test-connector", taskId) shouldBe true

      javaApi.deleteConnector("test-connector") shouldBe true

    }
  }

  private def withConnect(body: => Unit): Unit = {
    import scala.collection.JavaConverters._

    val listeners = s"http://localhost:$CONNECT_PORT"
    val connectProperties = Map[String, String](
      WorkerConfig.LISTENERS_CONFIG -> listeners,
      WorkerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
      WorkerConfig.KEY_CONVERTER_CLASS_CONFIG -> "org.apache.kafka.connect.json.JsonConverter",
      WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG -> "org.apache.kafka.connect.json.JsonConverter",
      WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG -> "10000",
      StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG -> File.makeTemp().jfile.getAbsolutePath
    )
    val standaloneConfig = new StandaloneConfig(connectProperties.asJava)
    val rest = new RestServer(standaloneConfig)
    rest.initializeServer()

    val plugins = new Plugins(connectProperties.asJava)
    plugins.compareAndSwapWithDelegatingLoader
    val connectorClientConfigOverridePolicy = plugins.newPlugin(
      standaloneConfig.getString(
        WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG
      ),
      standaloneConfig,
      classOf[ConnectorClientConfigOverridePolicy]
    )

    val workerId = s"localhost:$CONNECT_PORT"
    val worker = new Worker(
      workerId,
      Time.SYSTEM,
      plugins,
      standaloneConfig,
      new MemoryOffsetBackingStore,
      connectorClientConfigOverridePolicy
    )
    val clusterId = ConnectUtils.lookupKafkaClusterId(standaloneConfig)
    val herder = new StandaloneHerder(
      worker,
      clusterId,
      connectorClientConfigOverridePolicy
    )

    val kafkaConnect = new Connect(herder, rest)
    kafkaConnect.start()

    try {
      body
    } finally {
      kafkaConnect.stop()
    }
  }

}
