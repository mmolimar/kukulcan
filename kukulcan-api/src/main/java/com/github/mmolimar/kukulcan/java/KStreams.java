package com.github.mmolimar.kukulcan.java;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Map;
import java.util.Properties;

import static com.github.mmolimar.kukulcan.java.KUtils.toJavaMap;

public class KStreams extends KafkaStreams {

    private final com.github.mmolimar.kukulcan.KStreams kstreams;

    public KStreams(Topology topology, Properties props) {
        super(topology, props);
        this.kstreams = new com.github.mmolimar.kukulcan.KStreams(topology, props);
    }

    private KStreams(com.github.mmolimar.kukulcan.KStreams kstreams) {
        super(kstreams.topology(), kstreams.props());
        this.kstreams = kstreams;
    }

    public KStreams withApplicationId(String applicationId) {
        return new KStreams(this.kstreams.withApplicationId(applicationId));
    }

    public KStreams withProperties(Properties props) {
        return new KStreams(this.kstreams.withProperties(props));
    }

    public Map<MetricName, Metric> getMetrics() {
        return getMetrics(".*");
    }

    public Map<MetricName, Metric> getMetrics(String groupRegex) {
        return getMetrics(groupRegex, ".*");
    }

    public Map<MetricName, Metric> getMetrics(String groupRegex, String nameRegex) {
        return toJavaMap(this.kstreams.getMetrics(groupRegex, nameRegex));
    }

    public void listMetrics() {
        listMetrics(".*");
    }

    public void listMetrics(String groupRegex) {
        listMetrics(groupRegex, ".*");
    }

    public void listMetrics(String groupRegex, String nameRegex) {
        this.kstreams.listMetrics(groupRegex, nameRegex);
    }

    public void printTopology() {
        printTopology(true, true);
    }

    public void printTopology(boolean subtopologies, boolean globalStores) {
        this.kstreams.printTopology(subtopologies, globalStores);
    }

}
