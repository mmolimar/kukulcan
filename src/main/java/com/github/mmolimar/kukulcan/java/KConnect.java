package com.github.mmolimar.kukulcan.java;

import com.github.mmolimar.kukulcan.responses.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.github.mmolimar.kukulcan.java.KUtils.*;

public class KConnect {

    private final com.github.mmolimar.kukulcan.KConnect kconnect;

    public KConnect(Properties props) {
        this.kconnect = new com.github.mmolimar.kukulcan.KConnect(props);
    }

    public ServerVersion serverVersion() {
        return kconnect.serverVersion();
    }

    public Connector connector(String name) {
        return kconnect.connector(name);
    }

    public Map<String, String> connectorConfig(String name) {
        return toJavaMap(kconnect.connectorConfig(name));
    }

    public List<ConnectorPlugin> connectorPlugins() {
        return toJavaList(kconnect.connectorPlugins());
    }

    public ConnectorStatus connectorStatus(String name) {
        return kconnect.connectorStatus(name);
    }

    public List<Task> connectorTasks(String name) {
        return toJavaList(kconnect.connectorTasks(name));
    }

    public TaskStatus connectorTaskStatus(String name, int taskId) {
        return kconnect.connectorTaskStatus(name, taskId);
    }

    public ConnectorTopics connectorTopics(String name) {
        return kconnect.connectorTopics(name);
    }

    public ConnectorPluginValidation validateConnectorPluginConfig(String name, Map<String, String> config) {
        return kconnect.validateConnectorPluginConfig(name,
                (scala.collection.immutable.Map<String, String>) toScalaMap(config));
    }

    public Connector updateConnector(String name, Map<String, String> config) {
        return kconnect.updateConnector(name,
                (scala.collection.immutable.Map<String, String>) toScalaMap(config));
    }

    public boolean pauseConnector(String name) {
        return kconnect.pauseConnector(name);
    }

    public boolean resetConnectorTopics(String name) {
        return kconnect.resetConnectorTopics(name);
    }

    public boolean resumeConnector(String name) {
        return kconnect.resumeConnector(name);
    }

    public boolean restartConnector(String name) {
        return kconnect.restartConnector(name);
    }

    public boolean restartConnectorTask(String name, int taskId) {
        return kconnect.restartConnectorTask(name, taskId);
    }

    public boolean deleteConnector(String name) {
        return kconnect.deleteConnector(name);
    }

    public List<String> connectors() {
        return toJavaList(kconnect.connectors());
    }

    public ConnectorExpandedInfo connectorsWithExpandedInfo() {
        return kconnect.connectorsWithExpandedInfo();
    }

    public ConnectorExpandedStatus connectorsWithExpandedStatus() {
        return kconnect.connectorsWithExpandedStatus();
    }

    public ConnectorExpandedMetadata connectorsWithAllExpandedMetadata() {
        return kconnect.connectorsWithAllExpandedMetadata();
    }
}
