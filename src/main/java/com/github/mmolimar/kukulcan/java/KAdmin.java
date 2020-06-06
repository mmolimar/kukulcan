package com.github.mmolimar.kukulcan.java;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Authorizer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.github.mmolimar.kukulcan.java.KUtils.*;

@SuppressWarnings("WeakerAccess")
public class KAdmin {

    public final Admin admin;
    public final String servers;
    public final KAdminTopics topics;
    public final KAdminConfigs configs;
    public final KAdminAcls acls;
    public final KAdminMetrics metrics;

    KAdmin(com.github.mmolimar.kukulcan.KAdmin admin) {
        this.admin = admin.client();
        this.servers = admin.servers();
        this.configs = new KAdminConfigs(admin.configs());
        this.topics = new KAdminTopics(admin.topics());
        this.acls = new KAdminAcls(admin.acls());
        this.metrics = new KAdminMetrics(admin.metrics());
    }

    public static class KAdminTopics {
        private final com.github.mmolimar.kukulcan.KAdmin.KAdminTopics topics;

        KAdminTopics(com.github.mmolimar.kukulcan.KAdmin.KAdminTopics topics) {
            this.topics = topics;
        }

        public boolean createTopic(String name, int partitions, short replicationFactor, Map<String, String> configs) {
            NewTopic topic = new NewTopic(name, partitions, replicationFactor).configs(configs);
            return createTopics(Collections.singletonList(topic), new CreateTopicsOptions());
        }

        public boolean createTopic(String name, int partitions, short replicationFactor, Map<String, String> configs,
                                   CreateTopicsOptions options) {
            NewTopic topic = new NewTopic(name, partitions, replicationFactor).configs(configs);
            return createTopics(Collections.singletonList(topic), options);
        }

        public boolean createTopics(List<NewTopic> newTopics, CreateTopicsOptions options) {
            return topics.createTopics(toScalaSeq(newTopics), toScalaOption(options));
        }

        public boolean createTopicWithReplicasAssignments(String name, Map<Integer, List<Integer>> replicasAssignments,
                                                          Map<String, String> configs) {
            return createTopicWithReplicasAssignments(name, replicasAssignments, configs, new CreateTopicsOptions());
        }

        public boolean createTopicWithReplicasAssignments(String name, Map<Integer, List<Integer>> replicasAssignments,
                                                          Map<String, String> configs, CreateTopicsOptions options) {
            NewTopic topic = new NewTopic(name, replicasAssignments).configs(configs);
            return createTopics(Collections.singletonList(topic), options);
        }

        public List<String> getTopics(boolean excludeInternalTopics) {
            return toJavaList(topics.getTopics(toScalaOption(null), excludeInternalTopics));
        }

        public List<String> getTopics(String topicWhitelist, boolean excludeInternalTopics) {
            return toJavaList(topics.getTopics(toScalaOption(topicWhitelist), excludeInternalTopics));
        }

        public void listTopics(boolean excludeInternalTopics) {
            topics.listTopics(toScalaOption(null), excludeInternalTopics);
        }

        public void listTopics(String topicWhitelist, boolean excludeInternalTopics) {
            topics.listTopics(toScalaOption(topicWhitelist), excludeInternalTopics);
        }

        public void describeTopic(String name, boolean withConfigs, boolean withPartitions) {
            describeTopics(Collections.singletonList(name), new DescribeTopicsOptions(), withConfigs, withPartitions);
        }

        public void describeTopics(List<String> topics, DescribeTopicsOptions options,
                                   boolean withConfigs, boolean withPartitions) {
            this.topics.describeTopics(toScalaSeq(topics), toScalaOption(options), withConfigs, withPartitions);
        }

        public void deleteTopic(String name) {
            deleteTopics(Collections.singletonList(name));
        }

        public void deleteTopics(List<String> topics) {
            this.topics.deleteTopics(toScalaSeq(topics));
        }

        public Map<String, NewPartitions> getNewPartitionsDistribution(String topic, int partitions,
                                                                       Map<Integer, List<Integer>> replicaAssignment) {
            return getNewPartitionsDistribution(Collections.singletonList(topic), partitions, replicaAssignment);
        }

        public Map<String, NewPartitions> getNewPartitionsDistribution(List<String> topics, int partitions,
                                                                       Map<Integer, List<Integer>> replicaAssignment) {
            Object assignments = toScalaMap(
                    replicaAssignment.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, entry -> toScalaSeq(entry.getValue())))
            );
            //noinspection unchecked
            return toJavaMap(
                    this.topics.getNewPartitionsDistribution(
                            toScalaSeq(topics),
                            partitions,
                            (scala.collection.immutable.Map<Object, scala.collection.Seq<Object>>) assignments
                    )
            );
        }

        public void alterTopic(String topic, int partitions, Map<Integer, List<Integer>> replicaAssignment) {
            alterTopics(Collections.singletonList(topic), partitions, replicaAssignment);
        }

        public void alterTopics(List<String> topics, int partitions, Map<Integer, List<Integer>> replicaAssignment) {
            Object assignments = toScalaMap(
                    replicaAssignment.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, entry -> toScalaSeq(entry.getValue())))
            );
            //noinspection unchecked
            this.topics.alterTopics(
                    toScalaSeq(topics),
                    partitions,
                    (scala.collection.immutable.Map<Object, scala.collection.Seq<Object>>) assignments
            );
        }
    }

    public static class KAdminConfigs {

        private final com.github.mmolimar.kukulcan.KAdmin.KAdminConfigs configs;

        public KAdminConfigs(com.github.mmolimar.kukulcan.KAdmin.KAdminConfigs configs) {
            this.configs = configs;
        }

        public void alterTopicConfig(String name, Map<String, String> configsToBeAdded,
                                     List<String> configsToBeDeleted) {
            configs.alterTopicConfig(name, toScalaMap(configsToBeAdded), toScalaSeq(configsToBeDeleted));
        }

        public void alterBrokerConfig(int brokerId, Map<String, String> configsToBeAdded,
                                      List<String> configsToBeDeleted) {
            configs.alterBrokerConfig(toScalaOption(brokerId), toScalaMap(configsToBeAdded),
                    toScalaSeq(configsToBeDeleted));
        }

        public void alterBrokerConfig(Map<String, String> configsToBeAdded, List<String> configsToBeDeleted) {
            configs.alterBrokerConfig(toScalaOption(null), toScalaMap(configsToBeAdded), toScalaSeq(configsToBeDeleted));
        }

        public void alterBrokerLoggerConfig(int brokerId, Map<String, String> configsToBeAdded,
                                            List<String> configsToBeDeleted) {
            configs.alterBrokerLoggerConfig(toScalaOption(brokerId), toScalaMap(configsToBeAdded),
                    toScalaSeq(configsToBeDeleted));
        }

        public void alterBrokerLoggerConfig(Map<String, String> configsToBeAdded, List<String> configsToBeDeleted) {
            configs.alterBrokerLoggerConfig(toScalaOption(null), toScalaMap(configsToBeAdded),
                    toScalaSeq(configsToBeDeleted));
        }

        public List<ConfigEntry> getDescribeTopicConfig(String name, boolean describeAll) {
            return toJavaList(configs.getDescribeTopicConfig(toScalaOption(name), describeAll));
        }

        public List<ConfigEntry> getDescribeTopicConfig(boolean describeAll) {
            return toJavaList(configs.getDescribeTopicConfig(toScalaOption(null), describeAll));
        }

        public List<ConfigEntry> getDescribeBrokerConfig(int brokerId, boolean describeAll) {
            return toJavaList(configs.getDescribeBrokerConfig(toScalaOption(brokerId), describeAll));
        }

        public List<ConfigEntry> getDescribeBrokerConfig(boolean describeAll) {
            return toJavaList(configs.getDescribeBrokerConfig(toScalaOption(null), describeAll));
        }

        public List<ConfigEntry> getDescribeBrokerLoggerConfig(int brokerId, boolean describeAll) {
            return toJavaList(configs.getDescribeBrokerLoggerConfig(toScalaOption(brokerId), describeAll));
        }

        public List<ConfigEntry> getDescribeBrokerLoggerConfig(boolean describeAll) {
            return toJavaList(configs.getDescribeBrokerLoggerConfig(toScalaOption(null), describeAll));
        }

        public void describeTopicConfig(String name, boolean describeAll) {
            configs.describeTopicConfig(toScalaOption(name), describeAll);
        }

        public void describeTopicConfig(boolean describeAll) {
            configs.describeTopicConfig(toScalaOption(null), describeAll);
        }

        public void describeBrokerConfig(int brokerId, boolean describeAll) {
            configs.describeBrokerConfig(toScalaOption(brokerId), describeAll);
        }

        public void describeBrokerConfig(boolean describeAll) {
            configs.describeBrokerConfig(toScalaOption(null), describeAll);
        }

        public void describeBrokerLoggerConfig(int brokerId, boolean describeAll) {
            configs.describeBrokerConfig(toScalaOption(brokerId), describeAll);
        }

        public void describeBrokerLoggerConfig(boolean describeAll) {
            configs.describeBrokerConfig(toScalaOption(null), describeAll);
        }

    }

    @SuppressWarnings("WeakerAccess")
    public static class KAdminAcls {

        private final com.github.mmolimar.kukulcan.KAdmin.KAdminAcls acls;

        KAdminAcls(com.github.mmolimar.kukulcan.KAdmin.KAdminAcls acls) {
            this.acls = acls;
        }

        public Optional<Authorizer> defaultAuthorizer(boolean forceDefault) {
            return toJavaOptional(acls.defaultAuthorizer(forceDefault));
        }

        public void addAcls(List<AclBinding> aclBindings) {
            addAcls(aclBindings, new CreateAclsOptions());
        }

        public void addAcls(List<AclBinding> aclBindings, CreateAclsOptions options) {
            addAcls(aclBindings, options, true);
        }

        public void addAcls(List<AclBinding> aclBindings, boolean forceAuthorizer) {
            addAcls(aclBindings, new CreateAclsOptions(), forceAuthorizer);
        }

        public void addAcls(List<AclBinding> aclBindings, CreateAclsOptions options, boolean forceAuthorizer) {
            acls.addAcls(toScalaSeq(aclBindings), options, forceAuthorizer);
        }

        public void removeAcls(List<AclBindingFilter> filters) {
            removeAcls(filters, new DeleteAclsOptions());
        }

        public void removeAcls(List<AclBindingFilter> filters, DeleteAclsOptions options) {
            removeAcls(filters, options, true);
        }

        public void removeAcls(List<AclBindingFilter> filters, boolean forceAuthorizer) {
            removeAcls(filters, new DeleteAclsOptions(), forceAuthorizer);
        }

        public void removeAcls(List<AclBindingFilter> filters, DeleteAclsOptions options, boolean forceAuthorizer) {
            acls.removeAcls(toScalaSeq(filters), options, forceAuthorizer);
        }

        public List<AclBinding> getAcls() {
            return getAcls(AclBindingFilter.ANY, new DescribeAclsOptions());
        }

        public List<AclBinding> getAcls(AclBindingFilter filter, DescribeAclsOptions options) {
            return getAcls(filter, options, true);
        }

        public List<AclBinding> getAcls(AclBindingFilter filter, boolean forceAuthorizer) {
            return getAcls(filter, new DescribeAclsOptions(), forceAuthorizer);
        }

        public List<AclBinding> getAcls(AclBindingFilter filter, DescribeAclsOptions options, boolean forceAuthorizer) {
            return toJavaList(acls.getAcls(filter, options, forceAuthorizer));
        }

        public void listAcls() {
            listAcls(AclBindingFilter.ANY, new DescribeAclsOptions());
        }

        public void listAcls(AclBindingFilter filter, DescribeAclsOptions options) {
            listAcls(filter, Collections.emptyList(), options);
        }

        public void listAcls(AclBindingFilter filter, List<KafkaPrincipal> principals) {
            listAcls(filter, principals, new DescribeAclsOptions());
        }

        public void listAcls(AclBindingFilter filter, List<KafkaPrincipal> principals, DescribeAclsOptions options) {
            acls.listAcls(filter, toScalaSeq(principals), options);
        }
    }

    public static class KAdminMetrics {

        private final com.github.mmolimar.kukulcan.KAdmin.KAdminMetrics metrics;

        KAdminMetrics(com.github.mmolimar.kukulcan.KAdmin.KAdminMetrics metrics) {
            this.metrics = metrics;
        }

        public Map<MetricName, Metric> getMetrics() {
            return getMetrics(".*");
        }

        public Map<MetricName, Metric> getMetrics(String groupRegex) {
            return getMetrics(groupRegex, ".*");
        }

        public Map<MetricName, Metric> getMetrics(String groupRegex, String nameRegex) {
            return toJavaMap(metrics.getMetrics(groupRegex, nameRegex));
        }

        public void listMetrics() {
            listMetrics(".*");
        }

        public void listMetrics(String groupRegex) {
            listMetrics(groupRegex, ".*");
        }

        public void listMetrics(String groupRegex, String nameRegex) {
            metrics.listMetrics(groupRegex, nameRegex);
        }

    }
}
