package com.github.mmolimar.kukulcan

import _root_.java.io.File
import _root_.java.util.Properties

import org.sourcelab.kafka.connect.apiclient.request.dto.{ConnectServerVersion => JConnectServerVersion, ConnectorDefinition => JConnectorDefinition, ConnectorPlugin => JConnectorPlugin, ConnectorPluginConfigDefinition => JConnectorPluginConfigDefinition, ConnectorPluginConfigValidationResults => JConnectorPluginConfigValidationResults, ConnectorStatus => JConnectorStatus, ConnectorTopics => JConnectorTopics, ConnectorsWithExpandedInfo => JConnectorsWithExpandedInfo, ConnectorsWithExpandedMetadata => JConnectorsWithExpandedMetadata, ConnectorsWithExpandedStatus => JConnectorsWithExpandedStatus, NewConnectorDefinition => JNewConnectorDefinition, Task => JTask, TaskStatus => JTaskStatus}
import org.sourcelab.kafka.connect.apiclient.{Configuration, KafkaConnectClient}

/**
 * Factory for [[com.github.mmolimar.kukulcan.KConnect]] instances.
 *
 */
object KConnect {

  def apply(props: Properties): KConnect = new KConnect(props)

}

/**
 * A class to interact with Kafka Connect API via HTTP(s).
 *
 * @param props Properties with the configuration.
 */
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

  /**
   * Get the server version.
   *
   * @return A {@code ServerVersion} instance.
   */
  def serverVersion: ServerVersion = client.getConnectServerVersion

  /**
   * Add a new connector.
   *
   * @param name   Connector name.
   * @param config {@code Map} with the connector configurations.
   * @return A {@code Connector} with the connector definition.
   */
  def addConnector(name: String, config: Map[String, String]): Connector = {
    client.addConnector(new JNewConnectorDefinition(name, config.asJava))
  }

  /**
   * Get a connector.
   *
   * @param name Connector name.
   * @return A {@code Connector} with the connector definition.
   */
  def connector(name: String): Connector = client.getConnector(name)

  /**
   * Get the connector configurations.
   *
   * @param name Connector name.
   * @return A {@code Map} with the connector configurations.
   */
  def connectorConfig(name: String): Map[String, String] = client.getConnectorConfig(name).asScala.toMap

  /**
   * Get the available connector plugins.
   *
   * @return A {@code ConnectorPlugin} list with all plugins.
   */
  def connectorPlugins: Seq[ConnectorPlugin] = client.getConnectorPlugins.asScala.map(toConnectorPlugin).toSeq

  /**
   * Get the connector status.
   *
   * @param name Connector name.
   * @return A {@code ConnectorStatus} with the connector status.
   */
  def connectorStatus(name: String): ConnectorStatus = client.getConnectorStatus(name)

  /**
   * Get the connector tasks.
   *
   * @param name Connector name.
   * @return A {@code Task} list with its connector tasks.
   */
  def connectorTasks(name: String): Seq[Task] = client.getConnectorTasks(name).asScala.map(toTask).toSeq

  /**
   * Get the connector task status.
   *
   * @param name   Connector name.
   * @param taskId Task id to get the status.
   * @return A {@code Task} with its status.
   */
  def connectorTaskStatus(name: String, taskId: Int): TaskStatus = client.getConnectorTaskStatus(name, taskId)

  /**
   * Get the set of topics that a specific connector is using.
   *
   * @param name Connector name.
   * @return A {@code ConnectorTopics} with the topics used.
   */
  def connectorTopics(name: String): ConnectorTopics = client.getConnectorTopics(name)

  /**
   * Validate a connector plugin config.
   *
   * @param name   Connector name.
   * @param config Configuration values for the connector.
   * @return A {@code ConnectorPluginValidation} with the results of the validation
   */
  def validateConnectorPluginConfig(name: String, config: Map[String, String]): ConnectorPluginValidation = {
    client.validateConnectorPluginConfig(
      new JConnectorPluginConfigDefinition(name, config.asJava)
    )
  }

  /**
   * Update the connector's configuration.
   *
   * @param name   Connector name.
   * @param config Configuration values to set.
   * @return A {@code Connector} with the connector definition.
   */
  def updateConnector(name: String, config: Map[String, String]): Connector = {
    client.updateConnectorConfig(name, config.asJava)
  }

  /**
   * Pause a connector.
   *
   * @param name Connector name.
   * @return If the connector could be paused.
   */
  def pauseConnector(name: String): Boolean = client.pauseConnector(name)

  /**
   * Reset the active topics for the connector.
   *
   * @param name Connector name.
   * @return If the connector could be reset.
   */
  def resetConnectorTopics(name: String): Boolean = client.resetConnectorTopics(name)

  /**
   * Resume the connector.
   *
   * @param name Connector name.
   * @return If the connector could be resumed.
   */
  def resumeConnector(name: String): Boolean = client.resumeConnector(name)

  /**
   * Restart the connector.
   *
   * @param name Connector name.
   * @return If the connector could be restarted.
   */
  def restartConnector(name: String): Boolean = client.restartConnector(name)

  /**
   * Restart a task in the connector.
   *
   * @param name   Connector name.
   * @param taskId Task to restart.
   * @return If the task could be restarted.
   */
  def restartConnectorTask(name: String, taskId: Int): Boolean = client.restartConnectorTask(name, taskId)

  /**
   * Delete a connector.
   *
   * @param name Connector name.
   * @return If the task could be deleted.
   */
  def deleteConnector(name: String): Boolean = client.deleteConnector(name)

  /**
   * Get all connectors deployed.
   *
   * @return A list with the connector names.
   */
  def connectors: Seq[String] = client.getConnectors.asScala.toSeq

  /**
   * Get all connectors deployed, including the definition for each connector.
   *
   * @return A {@code ConnectorExpandedInfo} with the extended connector definition.
   */
  def connectorsWithExpandedInfo: ConnectorExpandedInfo = client.getConnectorsWithExpandedInfo

  /**
   * Get all connectors deployed, including the status for each connector.
   *
   * @return A { @code ConnectorExpandedStatus} with the extended status info.
   */
  def connectorsWithExpandedStatus: ConnectorExpandedStatus = client.getConnectorsWithExpandedStatus

  /**
   * Get all connectors deployed, including all metadata available.
   *
   * @return A { @code ConnectorExpandedMetadata} with all metadata available for each connector.
   */
  def connectorsWithAllExpandedMetadata: ConnectorExpandedMetadata = client.getConnectorsWithAllExpandedMetadata

}

object responses {

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
    /**
     * Convert the instance {@code T} to JSON format.
     *
     * @return The instance in JSON format
     */
    def toJson: String = convertToJson(this.asInstanceOf[T])(encoder)

    /**
     * Print the instance {@code T} in JSON format.
     *
     */
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
