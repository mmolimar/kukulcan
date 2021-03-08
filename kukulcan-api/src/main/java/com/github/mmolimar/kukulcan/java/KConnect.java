package com.github.mmolimar.kukulcan.java;

import com.github.mmolimar.kukulcan.responses.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.github.mmolimar.kukulcan.java.KUtils.*;

/**
 * A class to interact with Kafka Connect API via HTTP(s).
 */
public class KConnect {

    private final com.github.mmolimar.kukulcan.KConnect kconnect;

    /**
     * @param props Properties with the configuration.
     */
    public KConnect(Properties props) {
        this.kconnect = new com.github.mmolimar.kukulcan.KConnect(props);
    }

    /**
     * Get the server version.
     *
     * @return A {@code ServerVersion} instance.
     */
    public ServerVersion serverVersion() {
        return kconnect.serverVersion();
    }

    /**
     * Add a new connector.
     *
     * @param name   Connector name.
     * @param config {@code Map[String, String]} with the connector configurations.
     * @return A {@code Connector} with the connector definition.
     */
    public Connector addConnector(String name, Map<String, String> config) {
        return kconnect.addConnector(name, toScalaMap(config));
    }

    /**
     * Get a connector.
     *
     * @param name Connector name.
     * @return A {@code Connector} with the connector definition.
     */
    public Connector connector(String name) {
        return kconnect.connector(name);
    }

    /**
     * Get the connector configurations.
     *
     * @param name Connector name.
     * @return A {@code Map[String, String]} with the connector configurations.
     */
    public Map<String, String> connectorConfig(String name) {
        return toJavaMap(kconnect.connectorConfig(name));
    }

    /**
     * Get the available connector plugins.
     *
     * @return A {@code ConnectorPlugin} list with all plugins.
     */
    public List<ConnectorPlugin> connectorPlugins() {
        return toJavaList(kconnect.connectorPlugins());
    }

    /**
     * Get the connector status.
     *
     * @param name Connector name.
     * @return A {@code ConnectorStatus} with the connector status.
     */
    public ConnectorStatus connectorStatus(String name) {
        return kconnect.connectorStatus(name);
    }

    /**
     * Get the connector tasks.
     *
     * @param name Connector name.
     * @return A {@code Task} list with its connector tasks.
     */
    public List<Task> connectorTasks(String name) {
        return toJavaList(kconnect.connectorTasks(name));
    }

    /**
     * Get the connector task status.
     *
     * @param name   Connector name.
     * @param taskId Task id to get the status.
     * @return A {@code Task} with its status.
     */
    public TaskStatus connectorTaskStatus(String name, int taskId) {
        return kconnect.connectorTaskStatus(name, taskId);
    }

    /**
     * Get the set of topics that a specific connector is using.
     *
     * @param name Connector name.
     * @return A {@code ConnectorTopics} with the topics used.
     */
    public ConnectorTopics connectorTopics(String name) {
        return kconnect.connectorTopics(name);
    }

    /**
     * Validate a connector plugin config.
     *
     * @param pluginName Connector plugin name.
     * @param config     Configuration values for the connector.
     * @return A {@code ConnectorPluginValidation} with the results of the validation
     */
    public ConnectorPluginValidation validateConnectorPluginConfig(String pluginName, Map<String, String> config) {
        return kconnect.validateConnectorPluginConfig(pluginName, toScalaMap(config));
    }

    /**
     * Update the connector's configuration.
     *
     * @param name   Connector name.
     * @param config Configuration values to set.
     * @return A {@code Connector} with the connector definition.
     */
    public Connector updateConnector(String name, Map<String, String> config) {
        return kconnect.updateConnector(name, toScalaMap(config));
    }

    /**
     * Pause a connector.
     *
     * @param name Connector name.
     * @return If the connector could be paused.
     */
    public boolean pauseConnector(String name) {
        return kconnect.pauseConnector(name);
    }

    /**
     * Reset the active topics for the connector.
     *
     * @param name Connector name.
     * @return If the connector could be reset.
     */
    public boolean resetConnectorTopics(String name) {
        return kconnect.resetConnectorTopics(name);
    }

    /**
     * Resume the connector.
     *
     * @param name Connector name.
     * @return If the connector could be resumed.
     */
    public boolean resumeConnector(String name) {
        return kconnect.resumeConnector(name);
    }

    /**
     * Restart the connector.
     *
     * @param name Connector name.
     * @return If the connector could be restarted.
     */
    public boolean restartConnector(String name) {
        return kconnect.restartConnector(name);
    }

    /**
     * Restart a task in the connector.
     *
     * @param name   Connector name.
     * @param taskId Task to restart.
     * @return If the task could be restarted.
     */
    public boolean restartConnectorTask(String name, int taskId) {
        return kconnect.restartConnectorTask(name, taskId);
    }

    /**
     * Delete a connector.
     *
     * @param name Connector name.
     * @return If the task could be deleted.
     */
    public boolean deleteConnector(String name) {
        return kconnect.deleteConnector(name);
    }

    /**
     * Get all connectors deployed.
     *
     * @return A list with the connector names.
     */
    public List<String> connectors() {
        return toJavaList(kconnect.connectors());
    }

    /**
     * Get all connectors deployed, including the definition for each connector.
     *
     * @return A {@code ConnectorExpandedInfo} with the extended connector definition.
     */
    public ConnectorExpandedInfo connectorsWithExpandedInfo() {
        return kconnect.connectorsWithExpandedInfo();
    }

    /**
     * Get all connectors deployed, including the status for each connector.
     *
     * @return A {@code ConnectorExpandedStatus} with the extended status info.
     */
    public ConnectorExpandedStatus connectorsWithExpandedStatus() {
        return kconnect.connectorsWithExpandedStatus();
    }

    /**
     * Get all connectors deployed, including all metadata available.
     *
     * @return A {@code ConnectorExpandedMetadata} with all metadata available for each connector.
     */
    public ConnectorExpandedMetadata connectorsWithAllExpandedMetadata() {
        return kconnect.connectorsWithAllExpandedMetadata();
    }
}
