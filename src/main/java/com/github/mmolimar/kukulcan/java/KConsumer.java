package com.github.mmolimar.kukulcan.java;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.tools.ToolsUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class KConsumer<K, V> extends KafkaConsumer<K, V> {

    public KConsumer(Properties props) {
        super(props);
    }

    public void subscribe(String topic) {
        subscribe(Collections.singletonList(topic));
    }

    public Map<MetricName, Metric> getMetrics() {
        return getMetrics(".*");
    }

    public Map<MetricName, Metric> getMetrics(String groupRegex) {
        return getMetrics(groupRegex, ".*");
    }

    public Map<MetricName, Metric> getMetrics(String groupRegex, String nameRegex) {
        return metrics().entrySet().stream()
                .filter(metric -> metric.getKey().group().matches(groupRegex) &&
                        metric.getKey().name().matches(nameRegex))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public void listMetrics() {
        listMetrics(".*");
    }

    public void listMetrics(String groupRegex) {
        listMetrics(groupRegex, ".*");
    }

    public void listMetrics(String groupRegex, String nameRegex) {
        ToolsUtils.printMetrics(getMetrics(groupRegex, nameRegex));
    }
}
