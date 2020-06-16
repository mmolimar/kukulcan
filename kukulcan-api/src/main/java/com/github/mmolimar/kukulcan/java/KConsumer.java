package com.github.mmolimar.kukulcan.java;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.tools.ToolsUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * An enriched implementation of the {@code org.apache.kafka.clients.consumer.KafkaConsumer} class to
 * consume messages in Kafka.
 */
public class KConsumer<K, V> extends KafkaConsumer<K, V> {

    /**
     * @param props Properties with the configuration.
     */
    public KConsumer(Properties props) {
        super(props);
    }

    /**
     * Subscribe to a topic
     *
     * @param topic Topic name.
     */
    public void subscribe(String topic) {
        subscribe(Collections.singletonList(topic));
    }

    /**
     * Get all metrics registered.
     *
     * @return a { @code Map} with the all metrics registered.
     */
    public Map<MetricName, Metric> getMetrics() {
        return getMetrics(".*");
    }

    /**
     * Get all metrics registered filtered by the group regular expressions.
     *
     * @param groupRegex Regex to filter metrics by group name.
     * @return a { @code Map} with the all metrics registered filtered by the group regular expression.
     */
    public Map<MetricName, Metric> getMetrics(String groupRegex) {
        return getMetrics(groupRegex, ".*");
    }

    /**
     * Get all metrics registered filtered by the group and name regular expressions.
     *
     * @param groupRegex Regex to filter metrics by group name.
     * @param nameRegex  Regex to filter metrics by its name.
     * @return a { @code Map} with the all metrics registered filtered by the group and name regular expressions.
     */
    public Map<MetricName, Metric> getMetrics(String groupRegex, String nameRegex) {
        return metrics().entrySet().stream()
                .filter(metric -> metric.getKey().group().matches(groupRegex) &&
                        metric.getKey().name().matches(nameRegex))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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
        ToolsUtils.printMetrics(getMetrics(groupRegex, nameRegex));
    }
}
