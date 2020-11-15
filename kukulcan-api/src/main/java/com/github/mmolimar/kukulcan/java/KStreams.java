package com.github.mmolimar.kukulcan.java;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Map;
import java.util.Properties;

import static com.github.mmolimar.kukulcan.java.KUtils.toJavaMap;

/**
 * An enriched implementation of the {@code org.apache.kafka.streams.KafkaStreams} class to
 * manages streams in Kafka.
 */
public class KStreams extends KafkaStreams {

    private final com.github.mmolimar.kukulcan.KStreams kstreams;

    /**
     * @param topology Topology specifying the computational logic.
     * @param props    Properties with the configuration.
     */
    public KStreams(Topology topology, Properties props) {
        super(topology, props);
        this.kstreams = new com.github.mmolimar.kukulcan.KStreams(topology, props);
    }

    private KStreams(com.github.mmolimar.kukulcan.KStreams kstreams) {
        super(kstreams.topology(), kstreams.props());
        this.kstreams = kstreams;
    }

    /**
     * Create new instance with a new application id
     *
     * @param applicationId The new application id.
     * @return The new instance created.
     */
    public KStreams withApplicationId(String applicationId) {
        return new KStreams(this.kstreams.withApplicationId(applicationId));
    }

    /**
     * Create new instance with the properties specified.
     *
     * @param props The properties to create the instance.
     * @return The new instance created.
     */
    public KStreams withProperties(Properties props) {
        return new KStreams(this.kstreams.withProperties(props));
    }

    /**
     * Get all metrics registered.
     *
     * @return a {@code Map[MetricName, Metric]} with the all metrics registered.
     */
    public Map<MetricName, Metric> getMetrics() {
        return getMetrics(".*");
    }

    /**
     * Get all metrics registered filtered by the group regular expressions.
     *
     * @param groupRegex Regex to filter metrics by group name.
     * @return a {@code Map[MetricName, Metric]} with the all metrics registered filtered by the group regular
     * expression.
     */
    public Map<MetricName, Metric> getMetrics(String groupRegex) {
        return getMetrics(groupRegex, ".*");
    }

    /**
     * Get all metrics registered filtered by the group and name regular expressions.
     *
     * @param groupRegex Regex to filter metrics by group name.
     * @param nameRegex  Regex to filter metrics by its name.
     * @return a {@code Map[MetricName, Metric]} with the all metrics registered filtered by the group and name regular
     * expressions.
     */
    public Map<MetricName, Metric> getMetrics(String groupRegex, String nameRegex) {
        return toJavaMap(this.kstreams.getMetrics(groupRegex, nameRegex));
    }

    /**
     * Print all metrics.
     */
    public void listMetrics() {
        listMetrics(".*");
    }

    /**
     * Print all metrics filtered by the group regular expression.
     *
     * @param groupRegex Regex to filter metrics by group name.
     */
    public void listMetrics(String groupRegex) {
        listMetrics(groupRegex, ".*");
    }

    /**
     * Print all metrics filtered by the group and name regular expressions.
     *
     * @param groupRegex Regex to filter metrics by group name.
     * @param nameRegex  Regex to filter metrics by its name.
     */
    public void listMetrics(String groupRegex, String nameRegex) {
        this.kstreams.listMetrics(groupRegex, nameRegex);
    }

    /**
     * Print the topology in an ASCII graph.
     */
    public void printTopology() {
        printTopology(true, true);
    }

    /**
     * Print the topology in an ASCII graph.
     *
     * @param subtopologies If to include the subtopologies.
     * @param globalStores  If to include the global stores.
     */
    public void printTopology(boolean subtopologies, boolean globalStores) {
        this.kstreams.printTopology(subtopologies, globalStores);
    }

}
