package com.github.mmolimar.kukulcan

import java.io.File
import java.util.Properties

import org.sourcelab.kafka.connect.apiclient.request.dto.ConnectorPluginConfigValidationResults.{Config => ClientConnectorPluginValidationConfig}
import org.sourcelab.kafka.connect.apiclient.request.dto.{ConnectServerVersion => JConnectServerVersion, ConnectorDefinition => JConnectorDefinition, ConnectorPlugin => JConnectorPlugin, ConnectorPluginConfigDefinition => JConnectorPluginConfigDefinition, ConnectorPluginConfigValidationResults => JConnectorPluginConfigValidationResults, ConnectorStatus => JConnectorStatus, ConnectorTopics => JConnectorTopics, ConnectorsWithExpandedInfo => JConnectorsWithExpandedInfo, ConnectorsWithExpandedMetadata => JConnectorsWithExpandedMetadata, ConnectorsWithExpandedStatus => JConnectorsWithExpandedStatus, NewConnectorDefinition => JNewConnectorDefinition, Task => JTask, TaskStatus => JTaskStatus}
import org.sourcelab.kafka.connect.apiclient.{Configuration, KafkaConnectClient}

private[kukulcan] object KConnect extends Api[KConnect]("connect") {

  private def nonEmpty(key: String, props: Properties): Boolean = {
    props.containsKey(key) && props.getProperty(key).trim.nonEmpty
  }

  override protected def createInstance(props: Properties): KConnect = {
    val host = {
      s"${props.getProperty("rest.host.name", "localhost")}:${props.getProperty("rest.port", "8083")}"
    }
    val config = new Configuration(host)
    if (nonEmpty("rest.auth.basic.username", props) && nonEmpty("rest.auth.basic.password", props)) {
      config.useBasicAuth(
        props.getProperty("rest.auth.basic.username"),
        props.getProperty("rest.auth.basic.password")
      )
    }
    if (nonEmpty("proxy.host", props) && nonEmpty("proxy.port", props) && nonEmpty("proxy.scheme", props)) {
      config.useProxy(
        props.getProperty("proxy.host"),
        props.getProperty("proxy.port").toInt,
        props.getProperty("proxy.scheme")
      )
    }
    if (nonEmpty("proxy.auth.username", props)) {
      config.useProxyAuthentication(
        props.getProperty("proxy.auth.username"),
        props.getProperty("proxy.auth.password", "")
      )
    }
    if (nonEmpty("ssl.keystore.location", props)) {
      config.useKeyStore(
        new File(props.getProperty("ssl.keystore.location")),
        props.getProperty("ssl.keystore.password", "")
      )
    }
    if (nonEmpty("ssl.truststore.location", props)) {
      config.useTrustStore(
        new File(props.getProperty("ssl.truststore.location")),
        props.getProperty("ssl.truststore.password", "")
      )
    }
    if (nonEmpty("ssl.certificates.insecure", props) && props.getProperty("ssl.certificates.insecure").toBoolean) {
      config.useInsecureSslCertificates()
    }
    config.useRequestTimeoutInSeconds(props.getProperty("request.timeout.seconds", "300").toInt)

    KConnect(new KafkaConnectClient(config))
  }

}

private[kukulcan] case class KConnect(private val client: KafkaConnectClient) {

  import scala.collection.JavaConverters._

  case class ServerVersion(version: String, commit: String, clusterId: String)

  case class Connector(name: String, `type`: String, tasks: Seq[TaskId], config: Map[String, String])

  case class ConnectorPlugin(className: String, `type`: String, version: String)

  case class ConnectorPluginValidation(
                                        name: String,
                                        errorCount: Int,
                                        groups: Seq[String],
                                        configs: Seq[ClientConnectorPluginValidationConfig]
                                      )

  case class ConnectorStatus(name: String, `type`: String, connector: Map[String, String], tasks: Seq[TaskStatus])

  case class ConnectorTopics(name: String, topics: Seq[String])

  case class ConnectorExpandedInfo(
                                    connectorNames: Seq[String],
                                    definitions: Seq[Connector],
                                    mappedDefinitions: Map[String, Connector]
                                  )

  case class ConnectorExpandedStatus(
                                      connectorNames: Seq[String],
                                      statuses: Seq[ConnectorStatus],
                                      mappedStatuses: Map[String, ConnectorStatus]
                                    )

  case class ConnectorExpandedMetadata(
                                        connectorNames: Seq[String],
                                        definitions: Seq[Connector],
                                        mappedDefinitions: Map[String, Connector],
                                        statuses: Seq[ConnectorStatus],
                                        mappedStatuses: Map[String, ConnectorStatus]
                                      )

  case class TaskId(connector: String, task: Int)

  case class Task(id: TaskId, config: Map[String, String])

  case class TaskStatus(id: Int, state: String, workerId: String, trace: String)

  def serverVersion: ServerVersion = client.getConnectServerVersion

  def addConnector(name: String, config: Map[String, String]): Connector = {
    client.addConnector(new JNewConnectorDefinition(name, config.asJava))
  }

  def connector(name: String): Connector = client.getConnector(name)

  def connectorConfig(name: String): Map[String, String] = client.getConnectorConfig(name).asScala.toMap

  def connectorPlugins: Seq[ConnectorPlugin] = client.getConnectorPlugins.asScala.map(toConnectorPlugin).toSeq

  def connectorStatus(name: String): ConnectorStatus = client.getConnectorStatus(name)

  def connectorTasks(name: String): Seq[Task] = client.getConnectorTasks(name).asScala.map(toTask).toSeq

  def connectorTaskStatus(name: String, taskId: Int): TaskStatus = client.getConnectorTaskStatus(name, taskId)

  def connectorTopics(name: String): ConnectorTopics = client.getConnectorTopics(name)

  def connectorsWithExpandedInfo: ConnectorExpandedInfo = client.getConnectorsWithExpandedInfo

  def connectorsWithExpandedStatus: ConnectorExpandedStatus = client.getConnectorsWithExpandedStatus

  def connectorsWithAllExpandedMetadata: ConnectorExpandedMetadata = client.getConnectorsWithAllExpandedMetadata

  def validateConnectorPluginConfig(name: String, config: Map[String, String]): ConnectorPluginValidation = {
    client.validateConnectorPluginConfig(new JConnectorPluginConfigDefinition(name, config.asJava))
  }

  def updateConnector(name: String, config: Map[String, String]): Connector = {
    client.updateConnectorConfig(name, config.asJava)
  }

  def pauseConnector(name: String): Boolean = client.pauseConnector(name)

  def resumeConnector(name: String): Boolean = client.resumeConnector(name)

  def restartConnector(name: String): Boolean = client.restartConnector(name)

  def restartConnectorTask(name: String, taskId: Int): Boolean = client.restartConnectorTask(name, taskId)

  def deleteConnector(name: String): Boolean = client.deleteConnector(name)

  private implicit def toServerVersion(serverVersion: JConnectServerVersion): ServerVersion = {
    ServerVersion(
      version = serverVersion.getVersion,
      commit = serverVersion.getCommit,
      clusterId = serverVersion.getKafkaClusterId
    )
  }

  private implicit def toConnector(connector: JConnectorDefinition): Connector = {
    Connector(
      name = connector.getName,
      `type` = connector.getType,
      tasks = connector.getTasks.asScala.map(t => TaskId(t.getConnector, t.getTask)),
      config = connector.getConfig.asScala.toMap
    )
  }

  private implicit def toConnectorPlugin(connectorPlugin: JConnectorPlugin): ConnectorPlugin = {
    ConnectorPlugin(
      className = connectorPlugin.getClassName,
      `type` = connectorPlugin.getType,
      version = connectorPlugin.getVersion
    )
  }

  private implicit def toConnectorStatus(connectorStatus: JConnectorStatus): ConnectorStatus = {
    ConnectorStatus(
      name = connectorStatus.getName,
      `type` = connectorStatus.getType,
      connector = connectorStatus.getConnector.asScala.toMap,
      tasks = connectorStatus.getTasks.asScala.map(t => TaskStatus(t.getId, t.getState, t.getTrace, t.getWorkerId))
    )
  }

  private implicit def toConnectorTopics(connectorTopics: JConnectorTopics): ConnectorTopics = {
    ConnectorTopics(
      name = connectorTopics.getName,
      topics = connectorTopics.getTopics.asScala
    )
  }

  private implicit def toConnectorPluginValidation(validationResult: JConnectorPluginConfigValidationResults): ConnectorPluginValidation = {
    ConnectorPluginValidation(
      name = validationResult.getName,
      errorCount = validationResult.getErrorCount,
      groups = validationResult.getGroups.asScala.toSeq,
      configs = validationResult.getConfigs.asScala.toSeq
    )
  }

  private implicit def toConnectorExpandedInfo(expandedInfo: JConnectorsWithExpandedInfo): ConnectorExpandedInfo = {
    ConnectorExpandedInfo(
      connectorNames = expandedInfo.getConnectorNames.asScala.toSeq,
      definitions = expandedInfo.getAllDefinitions.asScala.map(toConnector).toSeq,
      mappedDefinitions = expandedInfo.getMappedDefinitions.asScala.map(s => s._1 -> toConnector(s._2)).toMap
    )
  }

  private implicit def toConnectorExpandedStatus(expandedStatus: JConnectorsWithExpandedStatus): ConnectorExpandedStatus = {
    ConnectorExpandedStatus(
      connectorNames = expandedStatus.getConnectorNames.asScala.toSeq,
      statuses = expandedStatus.getAllStatuses.asScala.map(toConnectorStatus).toSeq,
      mappedStatuses = expandedStatus.getMappedStatuses.asScala.map(s => s._1 -> toConnectorStatus(s._2)).toMap
    )
  }

  private implicit def toConnectorExpandedMetadata(expandedMetadata: JConnectorsWithExpandedMetadata): ConnectorExpandedMetadata = {
    ConnectorExpandedMetadata(
      connectorNames = expandedMetadata.getConnectorNames.asScala.toSeq,
      definitions = expandedMetadata.getAllDefinitions.asScala.map(toConnector).toSeq,
      mappedDefinitions = expandedMetadata.getMappedDefinitions.asScala.map(s => s._1 -> toConnector(s._2)).toMap,
      statuses = expandedMetadata.getAllStatuses.asScala.map(toConnectorStatus).toSeq,
      mappedStatuses = expandedMetadata.getMappedStatuses.asScala.map(s => s._1 -> toConnectorStatus(s._2)).toMap
    )
  }

  private implicit def toTask(task: JTask): Task = {
    Task(
      id = TaskId(task.getId.getConnector, task.getId.getTask),
      config = task.getConfig.asScala.toMap
    )
  }

  private implicit def toTaskStatus(taskStatus: JTaskStatus): TaskStatus = {
    TaskStatus(
      id = taskStatus.getId,
      state = taskStatus.getState,
      trace = taskStatus.getTrace,
      workerId = taskStatus.getWorkerId
    )
  }

}
