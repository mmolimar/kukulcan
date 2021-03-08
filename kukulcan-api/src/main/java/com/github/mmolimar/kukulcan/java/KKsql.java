package com.github.mmolimar.kukulcan.java;

import io.confluent.ksql.rest.client.StreamPublisher;
import io.confluent.ksql.rest.entity.*;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.github.mmolimar.kukulcan.java.KUtils.toJavaList;
import static com.github.mmolimar.kukulcan.java.KUtils.toScalaMap;
import static com.github.mmolimar.kukulcan.java.KUtils.scalaOption;

/**
 * An enriched implementation of the {@code io.confluent.ksql.rest.client.KsqlRestClient} class to execute
 * requests against KSQL.
 */
public class KKsql {

    private final com.github.mmolimar.kukulcan.KKsql kksql;

    /**
     * @param props Properties with the configuration.
     */
    public KKsql(Properties props) {
        this.kksql = new com.github.mmolimar.kukulcan.KKsql(props);
    }

    /**
     * Get the server URI for KSQL.
     *
     * @return an {@code URI} for the KSQL server.
     */
    public URI getServerAddress() {
        return kksql.getServerAddress();
    }

    /**
     * Get the server info for KSQL.
     *
     * @return a {@code ServerInfo} object with KSQL server info.
     */
    public ServerInfo getServerInfo() {
        return kksql.getServerInfo();
    }

    /**
     * Get the server metadata from KSQL.
     *
     * @return a {@code ServerMetadata} with the KSQL metadata.
     */
    public ServerMetadata getServerMetadata() {
        return kksql.getServerMetadata();
    }

    /**
     * Get the server metadata ID from KSQL.
     *
     * @return a {@code ServerClusterId} with the KSQL metadata ID.
     */
    public ServerClusterId getServerMetadataId() {
        return kksql.getServerMetadataId();
    }

    /**
     * Get the server health from KSQL server.
     *
     * @return a {@code HealthCheckResponse} with the health info.
     */
    public HealthCheckResponse getServerHealth() {
        return kksql.getServerHealth();
    }

    /**
     * Get the statuses for commands.
     *
     * @return a {@code CommandStatuses} with all statuses.
     */
    public CommandStatuses getAllStatuses() {
        return kksql.getAllStatuses();
    }

    /**
     * Get the status for a specified command.
     *
     * @param commandId The command ID to get its status.
     * @return a {@code CommandStatus} with the status for a command ID.
     */
    public CommandStatus getStatus(String commandId) {
        return kksql.getStatus(commandId);
    }

    /**
     * Make a heartbeat request to KSQL.
     *
     * @return a {@code HeartbeatResponse} with the heartbeat result.
     */
    public HeartbeatResponse makeHeartbeatRequest() {
        return kksql.makeHeartbeatRequest();
    }

    /**
     * Make a request to get the KSQL cluster status.
     *
     * @return a {@code ClusterStatusResponse} with the cluster status.
     */
    public ClusterStatusResponse makeClusterStatusRequest() {
        return kksql.makeClusterStatusRequest();
    }

    /**
     * Make a KSQL request with a command.
     *
     * @param ksql The command to request.
     * @return a list of {@code KsqlEntity} with the result.
     */
    public KsqlEntityList makeKsqlRequest(String ksql) {
        return kksql.makeKsqlRequest(ksql);
    }

    /**
     * Make a KSQL request with a command.
     *
     * @param ksql          The command to request.
     * @param commandSeqNum The previous command sequence number.
     * @return a list of {@code KsqlEntity} with the result.
     */
    public KsqlEntityList makeKsqlRequest(String ksql, Long commandSeqNum) {
        return kksql.makeKsqlRequest(ksql, commandSeqNum);
    }

    /**
     * Make a query into KSQL.
     *
     * @param ksql The query to request.
     * @return a {@code Seq[StreamedRow]} with the result.
     */
    public List<StreamedRow> makeQueryRequest(String ksql) {
        return makeQueryRequest(ksql, null);
    }

    /**
     * Make a query into KSQL.
     *
     * @param ksql          The query to request.
     * @param commandSeqNum The previous command sequence number.
     * @return a {@code Seq[StreamedRow]} with the result.
     */
    public List<StreamedRow> makeQueryRequest(String ksql, Long commandSeqNum) {
        return toJavaList(
                kksql.makeQueryRequest(
                        ksql,
                        scalaOption(commandSeqNum),
                        toScalaMap(Collections.emptyMap()),
                        toScalaMap(Collections.emptyMap())
                )
        );
    }

    /**
     * Make a query into KSQL.
     *
     * @param ksql       The query to request.
     * @param properties Custom properties to send to KSQL.
     * @return a {@code Seq[StreamedRow]} with the result.
     */
    public List<StreamedRow> makeQueryRequest(String ksql, Map<String, Object> properties, Map<String, Object> request) {
        return makeQueryRequest(ksql, null, properties, request);
    }

    /**
     * Make a query into KSQL.
     *
     * @param ksql          The query to request.
     * @param commandSeqNum The previous command sequence number.
     * @param properties    Custom properties to send to KSQL.
     * @return a {@code Seq[StreamedRow]} with the result.
     */
    public List<StreamedRow> makeQueryRequest(String ksql, Long commandSeqNum,
                                              Map<String, Object> properties, Map<String, Object> request) {
        return toJavaList(
                kksql.makeQueryRequest(ksql, scalaOption(commandSeqNum), toScalaMap(properties), toScalaMap(request))
        );
    }

    /**
     * Make a streamed query to KSQL.
     *
     * @param ksql The query to request.
     * @return a {@code StreamPublisher[StreamedRow]} with the result.
     */
    public StreamPublisher<StreamedRow> makeQueryRequestStreamed(String ksql) {
        return makeQueryRequestStreamed(ksql, null);
    }

    /**
     * Make a streamed query to KSQL.
     *
     * @param ksql          The query to request.
     * @param commandSeqNum The previous command sequence number.
     * @return a {@code StreamPublisher[StreamedRow]} with the result.
     */
    public StreamPublisher<StreamedRow> makeQueryRequestStreamed(String ksql, Long commandSeqNum) {
        return kksql.makeQueryRequestStreamed(ksql, scalaOption(commandSeqNum));
    }

    /**
     * Make a print topic request to KSQL.
     *
     * @param ksql The query to request.
     * @return a {@code List[String]} with the result.
     */
    public List<String> makePrintTopicRequest(String ksql) {
        return makePrintTopicRequest(ksql, null);
    }

    /**
     * Make a print topic request to KSQL.
     *
     * @param ksql          The query to request.
     * @param commandSeqNum The previous command sequence number.
     * @return a {@code List[String]} with the result.
     */
    public List<String> makePrintTopicRequest(String ksql, Long commandSeqNum) {
        return toJavaList(kksql.makePrintTopicRequest(ksql, scalaOption(commandSeqNum)));
    }

    /**
     * Make a print topic request to KSQL.
     *
     * @param ksql          The query to request.
     * @return a {@code StreamPublisher[StreamedRow]} with the result.
     */
    public StreamPublisher<String> makePrintTopicRequestStreamed(String ksql) {
        return makePrintTopicRequestStreamed(ksql, null);
    }

    /**
     * Make a print topic request to KSQL.
     *
     * @param ksql          The query to request.
     * @param commandSeqNum The previous command sequence number.
     * @return a {@code StreamPublisher[StreamedRow]} with the result.
     */
    public StreamPublisher<String> makePrintTopicRequestStreamed(String ksql, Long commandSeqNum) {
        return kksql.makePrintTopicRequestStreamed(ksql, scalaOption(commandSeqNum));
    }

    /**
     * Set a property into the KSQL Rest client.
     *
     * @param property The property name.
     * @param value    The value for this property.
     * @return the updated local properties.
     */
    public Object setProperty(String property, Object value) {
        return kksql.setProperty(property, value);
    }

    /**
     * Unset a property into the KSQL Rest client.
     *
     * @param property The property name.
     * @return the updated local properties.
     */
    public Object unsetProperty(String property) {
        return kksql.unsetProperty(property);
    }

}
