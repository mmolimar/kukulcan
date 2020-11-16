package com.github.mmolimar.kukulcan

import _root_.java.net.URI
import _root_.java.util.concurrent.TimeUnit
import _root_.java.util.{Optional, Properties}

import io.confluent.ksql.cli.Cli
import io.confluent.ksql.cli.console.OutputFormat
import io.confluent.ksql.properties.PropertiesUtil
import io.confluent.ksql.rest.client.{BasicCredentials, KsqlRestClient, RestResponse, StreamPublisher}
import io.confluent.ksql.rest.entity.{KsqlEntityList, StreamedRow, _}
import io.confluent.ksql.util.KsqlException

import scala.collection.JavaConverters._


/**
 * Factory for [[com.github.mmolimar.kukulcan.KKsql]] instances.
 *
 */
object KKsql {

  def apply[K, V](props: Properties): KKsql = new KKsql(props)

}

/**
 * An enriched implementation of the {@code io.confluent.ksql.rest.client.KsqlRestClient} class
 * to execute requests against KSQL.
 *
 * @param props Properties with the configuration.
 */
class KKsql(val props: Properties) {

  import scala.language.implicitConversions

  private val defaultTimeout = 10000L
  private val client: KsqlRestClient = fromProps(props)

  private def fromProps(props: Properties): KsqlRestClient = {
    val systemProps = System.getProperties
    val scalaProps = props.asScala.map {
      case (k, v) => k -> v.toString
    }.toMap

    val serverAddress = props.getProperty("ksql.server")
    val clientProps = PropertiesUtil.applyOverrides(scalaProps.asJava, systemProps)
    val credentials = if (props.contains("ksql.credentials.user") && props.contains("ksql.credentials.password")) {
      Optional.ofNullable(BasicCredentials.of(
        props.getProperty("ksql.credentials.user"),
        props.getProperty("ksql.credentials.password"))
      )
    } else {
      Optional.empty[BasicCredentials]()
    }
    KsqlRestClient.create(serverAddress, Map.empty[String, String].asJava, clientProps, credentials)
  }

  /**
   * Get the server URI for KSQL.
   *
   * @return an {@code URI} for the KSQL server.
   */
  def getServerAddress: URI = client.getServerAddress

  /**
   * Get the server info for KSQL.
   *
   * @return a {@code ServerInfo} object with KSQL server info.
   */
  def getServerInfo: ServerInfo = client.getServerInfo

  /**
   * Get the server metadata from KSQL.
   *
   * @return a {@code ServerMetadata} with the KSQL metadata.
   */
  def getServerMetadata: ServerMetadata = client.getServerMetadata

  /**
   * Get the server metadata ID from KSQL.
   *
   * @return a {@code ServerClusterId} with the KSQL metadata ID.
   */
  def getServerMetadataId: ServerClusterId = client.getServerMetadataId

  /**
   * Get the server health from KSQL server.
   *
   * @return a {@code HealthCheckResponse} with the health info.
   */
  def getServerHealth: HealthCheckResponse = client.getServerHealth

  /**
   * Get the statuses for commands.
   *
   * @return a {@code CommandStatuses} with all statuses.
   */
  def getAllStatuses: CommandStatuses = client.getAllStatuses

  /**
   * Get the status for a specified command.
   *
   * @param commandId The command ID to get its status.
   * @return a {@code CommandStatus} with the status for a command ID.
   */
  def getStatus(commandId: String): CommandStatus = client.getStatus(commandId)

  /**
   * Make a heartbeat request to KSQL.
   *
   * @return a {@code HeartbeatResponse} with the heartbeat result.
   */
  def makeHeartbeatRequest: HeartbeatResponse = {
    val hostInfo = new KsqlHostInfoEntity(getServerAddress.getHost, getServerAddress.getPort)
    client.makeAsyncHeartbeatRequest(hostInfo, System.currentTimeMillis).get(defaultTimeout, TimeUnit.MILLISECONDS)
  }

  /**
   * Make a request to get the KSQL cluster status.
   *
   * @return a {@code ClusterStatusResponse} with the cluster status.
   */
  def makeClusterStatusRequest: ClusterStatusResponse = client.makeClusterStatusRequest()

  /**
   * Make a KSQL request with a command.
   *
   * @param ksql The command to request.
   * @return a list of {@code KsqlEntity} with the result.
   */
  def makeKsqlRequest(ksql: String): KsqlEntityList = client.makeKsqlRequest(ksql)

  /**
   * Make a KSQL request with a command.
   *
   * @param ksql          The command to request.
   * @param commandSeqNum The previous command sequence number.
   * @return a list of {@code KsqlEntity} with the result.
   */
  def makeKsqlRequest(ksql: String, commandSeqNum: Long): KsqlEntityList = client.makeKsqlRequest(ksql, commandSeqNum)

  /**
   * Make a query into KSQL.
   *
   * @param ksql          The query to request.
   * @param commandSeqNum The previous command sequence number.
   * @return a {@code Seq[StreamedRow]} with the result.
   */
  def makeQueryRequest(ksql: String, commandSeqNum: Long): Seq[StreamedRow] = {
    manageResponse(client.makeQueryRequest(ksql, commandSeqNum)).asScala
  }

  /**
   * Make a query into KSQL.
   *
   * @param ksql          The query to request.
   * @param commandSeqNum The previous command sequence number.
   * @param properties    Custom properties to send to KSQL.
   * @return a {@code Seq[StreamedRow]} with the result.
   */
  def makeQueryRequest(ksql: String, commandSeqNum: Long, properties: Map[String, AnyRef]): Seq[StreamedRow] = {
    manageResponse(client.makeQueryRequest(ksql, commandSeqNum, properties.asJava)).asScala
  }

  /**
   * Make a streamed query to KSQL.
   *
   * @param ksql          The query to request.
   * @param commandSeqNum The previous command sequence number.
   * @return a {@code StreamPublisher[StreamedRow]} with the result.
   */
  def makeQueryRequestStreamed(ksql: String, commandSeqNum: Long): StreamPublisher[StreamedRow] = {
    client.makeQueryRequestStreamed(ksql, commandSeqNum)
  }

  /**
   * Make a print topic request to KSQL.
   *
   * @param ksql          The query to request.
   * @param commandSeqNum The previous command sequence number.
   * @return a {@code StreamPublisher[StreamedRow]} with the result.
   */
  def makePrintTopicRequest(ksql: String, commandSeqNum: Long): StreamPublisher[String] = {
    client.makePrintTopicRequest(ksql, commandSeqNum)
  }

  /**
   * Set a property into the KSQL Rest client.
   *
   * @param property The property name.
   * @param value    The value for this property.
   * @return the updated local properties.
   */
  def setProperty(property: String, value: AnyRef): AnyRef = client.setProperty(property, value)

  /**
   * Unset a property into the KSQL Rest client.
   *
   * @param property The property name.
   * @return the updated local properties.
   */
  def unsetProperty(property: String): AnyRef = client.unsetProperty(property)

  /**
   * Start the KSQL CLI.
   *
   */
  def console(): Unit = {
    val streamQueryRowLimit = props.getOrDefault("ksql.query.stream.row.limit", "100").toString.toLong
    val streamQueryTimeout = props.getOrDefault("ksql.query.timeout", "10000").toString.toLong
    val outputFormat = OutputFormat.valueOf(props.getOrDefault("ksql.output.format", "TABULAR").toString.toUpperCase)
    val cli = Cli.build(streamQueryRowLimit, streamQueryTimeout, outputFormat, client)
    cli.runInteractively()
  }

  private implicit def manageResponse[T](response: RestResponse[T]): T = {
    if (response.isErroneous) {
      val error = response.getErrorMessage
      throw new KsqlException(s"Error executing command in KSQL. " +
        s"Error code[${error.getErrorCode}]. Error message: ${error.getMessage}")
    }
    response.getResponse
  }

}
