package com.github.mmolimar.kukulcan

import _root_.java.io.File
import _root_.java.util.Properties

import org.sourcelab.kafka.connect.apiclient.request.dto.{ConnectServerVersion => JConnectServerVersion, ConnectorDefinition => JConnectorDefinition, ConnectorPlugin => JConnectorPlugin, ConnectorPluginConfigDefinition => JConnectorPluginConfigDefinition, ConnectorPluginConfigValidationResults => JConnectorPluginConfigValidationResults, ConnectorStatus => JConnectorStatus, ConnectorTopics => JConnectorTopics, ConnectorsWithExpandedInfo => JConnectorsWithExpandedInfo, ConnectorsWithExpandedMetadata => JConnectorsWithExpandedMetadata, ConnectorsWithExpandedStatus => JConnectorsWithExpandedStatus, NewConnectorDefinition => JNewConnectorDefinition, Task => JTask, TaskStatus => JTaskStatus}
import org.sourcelab.kafka.connect.apiclient.{Configuration, KafkaConnectClient}

private[kukulcan] object KConnect extends Api[KConnect]("connect") {

  override protected def createInstance(props: Properties): KConnect = {
    new KConnect(props)
  }

}

class KConnect(val props: Properties) {

  import responses._
  import responses.implicits._

  import scala.collection.JavaConverters._

  private val client: KafkaConnectClient = fromProps(props)

  private def fromProps(props: Properties): KafkaConnectClient = {
    def nonEmpty(key: String, props: Properties): Boolean = {
      props.containsKey(key) && props.getProperty(key).trim.nonEmpty
    }

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

    new KafkaConnectClient(config)
  }

  def reload(): Unit = KConnect.reload()

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

  def validateConnectorPluginConfig(name: String, config: Map[String, String]): ConnectorPluginValidation = {
    client.validateConnectorPluginConfig(
      new JConnectorPluginConfigDefinition(name, config.asJava)
    )
  }

  def updateConnector(name: String, config: Map[String, String]): Connector = {
    client.updateConnectorConfig(name, config.asJava)
  }

  def pauseConnector(name: String): Boolean = client.pauseConnector(name)

  def resetConnectorTopics(name: String): Boolean = client.resetConnectorTopics(name)

  def resumeConnector(name: String): Boolean = client.resumeConnector(name)

  def restartConnector(name: String): Boolean = client.restartConnector(name)

  def restartConnectorTask(name: String, taskId: Int): Boolean = client.restartConnectorTask(name, taskId)

  def deleteConnector(name: String): Boolean = client.deleteConnector(name)

  def connectors: Seq[String] = client.getConnectors.asScala.toSeq

  def connectorsWithExpandedInfo: ConnectorExpandedInfo = client.getConnectorsWithExpandedInfo

  def connectorsWithExpandedStatus: ConnectorExpandedStatus = client.getConnectorsWithExpandedStatus

  def connectorsWithAllExpandedMetadata: ConnectorExpandedMetadata = client.getConnectorsWithAllExpandedMetadata

}

private[kukulcan] object responses {

  import io.circe._
  import io.circe.generic.semiauto._
  import io.circe.syntax._

  import scala.collection.JavaConverters._

  private implicit val serverVersionEncoder: Encoder[ServerVersion] = deriveEncoder[ServerVersion]
  private implicit val connectorEncoder: Encoder[Connector] = deriveEncoder[Connector]
  private implicit val connectorPluginEncoder: Encoder[ConnectorPlugin] = deriveEncoder[ConnectorPlugin]
  private implicit val configDefinitionEncoder: Encoder[ConfigDefinition] = deriveEncoder[ConfigDefinition]
  private implicit val configValueEncoder: Encoder[ConfigValue] = deriveEncoder[ConfigValue]
  private implicit val connectorPluginValidationConfigEncoder: Encoder[ConnectorPluginValidationConfig] = deriveEncoder[ConnectorPluginValidationConfig]
  private implicit val connectorPluginValidationEncoder: Encoder[ConnectorPluginValidation] = deriveEncoder[ConnectorPluginValidation]
  private implicit val connectorStatusEncoder: Encoder[ConnectorStatus] = deriveEncoder[ConnectorStatus]
  private implicit val connectorTopicsEncoder: Encoder[ConnectorTopics] = deriveEncoder[ConnectorTopics]
  private implicit val connectorExpandedInfoEncoder: Encoder[ConnectorExpandedInfo] = deriveEncoder[ConnectorExpandedInfo]
  private implicit val connectorExpandedStatusEncoder: Encoder[ConnectorExpandedStatus] = deriveEncoder[ConnectorExpandedStatus]
  private implicit val connectorExpandedMetadataEncoder: Encoder[ConnectorExpandedMetadata] = deriveEncoder[ConnectorExpandedMetadata]
  private implicit val taskIdEncoder: Encoder[TaskId] = deriveEncoder[TaskId]
  private implicit val taskEncoder: Encoder[Task] = deriveEncoder[Task]
  private implicit val taskStatusEncoder: Encoder[TaskStatus] = deriveEncoder[TaskStatus]

  protected abstract class JsonSupport[T](implicit encoder: Encoder[T]) {
    def toJson: String = convertToJson(this.asInstanceOf[T])(encoder)

    def printJson(): Unit = println(toJson)
  }

  private def convertToJson[T](obj: T)(implicit encoder: Encoder[T]): String = obj.asJson.spaces2

  case class ServerVersion(version: String, commit: String, clusterId: String) extends JsonSupport[ServerVersion]

  case class Connector(
                        name: String,
                        `type`: String,
                        tasks: Seq[TaskId],
                        config: Map[String, String]
                      ) extends JsonSupport[Connector]

  case class ConnectorPlugin(className: String, `type`: String, version: String) extends JsonSupport[ConnectorPlugin]

  case class ConfigDefinition(
                               name: String,
                               `type`: String,
                               required: Boolean,
                               defaultValue: String,
                               importance: String,
                               documentation: String,
                               group: String,
                               width: String,
                               displayName: String,
                               dependents: Seq[String],
                               order: Int
                             ) extends JsonSupport[ConfigDefinition]

  case class ConfigValue(
                          name: String,
                          value: String,
                          recommendedValues: Seq[String],
                          errors: Seq[String],
                          visible: Boolean
                        ) extends JsonSupport[ConfigValue]

  case class ConnectorPluginValidationConfig(
                                              definition: ConfigDefinition,
                                              value: ConfigValue
                                            ) extends JsonSupport[ConnectorPluginValidationConfig]

  case class ConnectorPluginValidation(
                                        name: String,
                                        errorCount: Int,
                                        groups: Seq[String],
                                        configs: Seq[ConnectorPluginValidationConfig]
                                      ) extends JsonSupport[ConnectorPluginValidation]

  case class ConnectorStatus(
                              name: String,
                              `type`: String,
                              connector: Map[String, String],
                              tasks: Seq[TaskStatus]
                            ) extends JsonSupport[ConnectorStatus]

  case class ConnectorTopics(name: String, topics: Seq[String]) extends JsonSupport[ConnectorTopics]

  case class ConnectorExpandedInfo(
                                    connectorNames: Seq[String],
                                    definitions: Seq[Connector],
                                    mappedDefinitions: Map[String, Connector]
                                  ) extends JsonSupport[ConnectorExpandedInfo]

  case class ConnectorExpandedStatus(
                                      connectorNames: Seq[String],
                                      statuses: Seq[ConnectorStatus],
                                      mappedStatuses: Map[String, ConnectorStatus]
                                    ) extends JsonSupport[ConnectorExpandedStatus]

  case class ConnectorExpandedMetadata(
                                        connectorNames: Seq[String],
                                        definitions: Seq[Connector],
                                        mappedDefinitions: Map[String, Connector],
                                        statuses: Seq[ConnectorStatus],
                                        mappedStatuses: Map[String, ConnectorStatus]
                                      ) extends JsonSupport[ConnectorExpandedMetadata]

  case class TaskId(connector: String, task: Int) extends JsonSupport[TaskId]

  case class Task(id: TaskId, config: Map[String, String]) extends JsonSupport[Task]

  case class TaskStatus(id: Int, state: String, workerId: String, trace: String) extends JsonSupport[TaskStatus]

  object implicits {

    import scala.language.implicitConversions

    implicit def toServerVersion(serverVersion: JConnectServerVersion): ServerVersion = ServerVersion(
      version = serverVersion.getVersion,
      commit = serverVersion.getCommit,
      clusterId = serverVersion.getKafkaClusterId
    )

    implicit def toConnector(connector: JConnectorDefinition): Connector = Connector(
      name = connector.getName,
      `type` = connector.getType,
      tasks = connector.getTasks.asScala.map(t => TaskId(t.getConnector, t.getTask)),
      config = connector.getConfig.asScala.toMap
    )

    implicit def toConnectorPlugin(connectorPlugin: JConnectorPlugin): ConnectorPlugin = ConnectorPlugin(
      className = connectorPlugin.getClassName,
      `type` = connectorPlugin.getType,
      version = connectorPlugin.getVersion
    )

    implicit def toConnectorStatus(connectorStatus: JConnectorStatus): ConnectorStatus = ConnectorStatus(
      name = connectorStatus.getName,
      `type` = connectorStatus.getType,
      connector = connectorStatus.getConnector.asScala.toMap,
      tasks = connectorStatus.getTasks.asScala.map(t => TaskStatus(t.getId, t.getState, t.getTrace, t.getWorkerId)
      )
    )

    implicit def toConnectorTopics(connectorTopics: JConnectorTopics): ConnectorTopics = ConnectorTopics(
      name = connectorTopics.getName,
      topics = connectorTopics.getTopics.asScala
    )

    implicit def toConnectorPluginValidation(
                                              validationResult: JConnectorPluginConfigValidationResults
                                            ): ConnectorPluginValidation = ConnectorPluginValidation(
      name = validationResult.getName,
      errorCount = validationResult.getErrorCount,
      groups = validationResult.getGroups.asScala.toSeq,
      configs = validationResult.getConfigs.asScala.map { vr =>
        val definition = ConfigDefinition(
          name = vr.getDefinition.getName,
          `type` = vr.getDefinition.getType,
          required = vr.getDefinition.isRequired,
          defaultValue = vr.getDefinition.getDefaultValue,
          importance = vr.getDefinition.getImportance,
          documentation = vr.getDefinition.getDocumentation,
          group = vr.getDefinition.getGroup,
          width = vr.getDefinition.getWidth,
          displayName = vr.getDefinition.getDisplayName,
          dependents = vr.getDefinition.getDependents.asScala.toSeq,
          order = vr.getDefinition.getOrder
        )
        val value = ConfigValue(
          name = vr.getValue.getName,
          value = vr.getValue.getValue,
          recommendedValues = vr.getValue.getRecommendedValues.asScala.toSeq,
          errors = vr.getValue.getErrors.asScala.toSeq,
          visible = vr.getValue.isVisible
        )
        ConnectorPluginValidationConfig(definition, value)
      }.toSeq
    )

    implicit def toConnectorExpandedInfo(expandedInfo: JConnectorsWithExpandedInfo): ConnectorExpandedInfo =
      ConnectorExpandedInfo(
        connectorNames = expandedInfo.getConnectorNames.asScala.toSeq,
        definitions = expandedInfo.getAllDefinitions.asScala.map(toConnector).toSeq,
        mappedDefinitions = expandedInfo.getMappedDefinitions.asScala.map(s => s._1 -> toConnector(s._2)).toMap
      )

    implicit def toConnectorExpandedStatus(expandedStatus: JConnectorsWithExpandedStatus): ConnectorExpandedStatus =
      ConnectorExpandedStatus(
        connectorNames = expandedStatus.getConnectorNames.asScala.toSeq,
        statuses = expandedStatus.getAllStatuses.asScala.map(toConnectorStatus).toSeq,
        mappedStatuses = expandedStatus.getMappedStatuses.asScala.map(s => s._1 -> toConnectorStatus(s._2)).toMap
      )

    implicit def toConnectorExpandedMetadata(metadata: JConnectorsWithExpandedMetadata): ConnectorExpandedMetadata =
      ConnectorExpandedMetadata(
        connectorNames = metadata.getConnectorNames.asScala.toSeq,
        definitions = metadata.getAllDefinitions.asScala.map(toConnector).toSeq,
        mappedDefinitions = metadata.getMappedDefinitions.asScala.map(s => s._1 -> toConnector(s._2)).toMap,
        statuses = metadata.getAllStatuses.asScala.map(toConnectorStatus).toSeq,
        mappedStatuses = metadata.getMappedStatuses.asScala.map(s => s._1 -> toConnectorStatus(s._2)).toMap
      )

    implicit def toTask(task: JTask): Task = Task(
      id = TaskId(task.getId.getConnector, task.getId.getTask),
      config = task.getConfig.asScala.toMap
    )

    implicit def toTaskStatus(taskStatus: JTaskStatus): TaskStatus = TaskStatus(
      id = taskStatus.getId,
      state = taskStatus.getState,
      trace = taskStatus.getTrace,
      workerId = taskStatus.getWorkerId
    )
  }

}
