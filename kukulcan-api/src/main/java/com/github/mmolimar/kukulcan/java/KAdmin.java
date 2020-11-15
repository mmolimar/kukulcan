package com.github.mmolimar.kukulcan.java;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Authorizer;

import java.util.*;
import java.util.stream.Collectors;

import static com.github.mmolimar.kukulcan.java.KUtils.*;

/**
 * An enriched implementation of the {@code org.apache.kafka.clients.admin.KafkaAdminClient} class for
 * administrative operations in Kafka.
 * <p>
 * Provides a set of tools grouped by functionality: {@code topics}, {@code configs}, {@code acls} and
 * {@code metrics}.
 */
public class KAdmin {

    private final String servers;
    private final Admin client;
    private final KAdminTopics topics;
    private final KAdminConfigs configs;
    private final KAdminAcls acls;
    private final KAdminMetrics metrics;

    /**
     * @param props Properties with the configuration.
     */
    KAdmin(Properties props) {
        com.github.mmolimar.kukulcan.KAdmin admin = new com.github.mmolimar.kukulcan.KAdmin(props);
        this.servers = admin.servers();
        this.client = admin.client();
        this.configs = new KAdminConfigs(admin.configs());
        this.topics = new KAdminTopics(admin.topics());
        this.acls = new KAdminAcls(admin.acls());
        this.metrics = new KAdminMetrics(admin.metrics());
    }

    /**
     * Broker servers to connect to.
     *
     * @return The broker servers.
     */
    public String servers() {
        return servers;
    }

    /**
     * Native {@code org.apache.kafka.clients.admin.Admin} client used by this class.
     *
     * @return The {@code Admin} client instance.
     */
    public Admin client() {
        return client;
    }

    /**
     * An instance of the {@code KAdminTopics} to manage topics.
     *
     * @return The {@code KAdminTopics} instance.
     */
    public KAdminTopics topics() {
        return topics;
    }

    /**
     * An instance of the {@code KAdminConfigs} to manage configurations.
     *
     * @return The {@code KAdminConfigs} instance.
     */
    public KAdminConfigs configs() {
        return configs;
    }

    /**
     * An instance of the {@code KAdminAcls} to manage ACLs.
     *
     * @return The {@code KAdminAcls} instance.
     */
    public KAdminAcls acls() {
        return acls;
    }

    /**
     * An instance of the {@code KAdminMetrics} to see the metrics.
     *
     * @return The {@code KAdminMetrics} instance.
     */
    public KAdminMetrics metrics() {
        return metrics;
    }

    /**
     * Class which includes all administrative functionalities related with topics.
     */
    public static class KAdminTopics {
        private final com.github.mmolimar.kukulcan.KAdmin.KAdminTopics topics;

        KAdminTopics(com.github.mmolimar.kukulcan.KAdmin.KAdminTopics topics) {
            this.topics = topics;
        }

        /**
         * Create a new topic.
         * This operation is not transactional so it may succeed for some topics while fail for others.
         *
         * @param name              Topic name.
         * @param partitions        Number of partitions.
         * @param replicationFactor Replication factor.
         * @param configs           Extra configuration options for the topic.
         * @return If the topic was created or not.
         */
        public boolean createTopic(String name, int partitions, short replicationFactor, Map<String, String> configs) {
            NewTopic topic = new NewTopic(name, partitions, replicationFactor).configs(configs);
            return createTopics(Collections.singletonList(topic), new CreateTopicsOptions());
        }

        /**
         * Create a new topic.
         * This operation is not transactional so it may succeed for some topics while fail for others.
         *
         * @param name              Topic name.
         * @param partitions        Number of partitions.
         * @param replicationFactor Replication factor.
         * @param configs           Extra configuration options for the topic.
         * @param options           Extra create topics options.
         * @return If the topic was created or not.
         */
        public boolean createTopic(String name, int partitions, short replicationFactor, Map<String, String> configs,
                                   CreateTopicsOptions options) {
            NewTopic topic = new NewTopic(name, partitions, replicationFactor).configs(configs);
            return createTopics(Collections.singletonList(topic), options);
        }

        /**
         * Create a batch of new topics.
         * This operation is not transactional so it may succeed for some topics while fail for others.
         *
         * @param newTopics Topic list.
         * @param options   Extra create topics options.
         * @return If the topics were created or not.
         */
        public boolean createTopics(List<NewTopic> newTopics, CreateTopicsOptions options) {
            return topics.createTopics(toScalaSeq(newTopics), scalaOption(options));
        }

        /**
         * Create a new topic.
         * This operation is not transactional so it may succeed for some topics while fail for others.
         *
         * @param name               Topic name.
         * @param replicaAssignments A {@code Map[Integer, List[Integer]]} from partition id to replica ids.
         * @param configs            A {@code Map[String, String]} with extra configuration options for the topic.
         * @return If the topic was created or not.
         */
        public boolean createTopicWithReplicasAssignments(String name, Map<Integer, List<Integer>> replicaAssignments,
                                                          Map<String, String> configs) {
            return createTopicWithReplicasAssignments(name, replicaAssignments, configs, new CreateTopicsOptions());
        }

        /**
         * Create a new topic.
         * This operation is not transactional so it may succeed for some topics while fail for others.
         *
         * @param name               Topic name.
         * @param replicaAssignments A {@code Map[Integer, List[Integer]]} from partition id to replica ids.
         * @param configs            A {@code Map[String, String]} with extra configuration options for the topic.
         * @param options            Extra create topics options.
         * @return If the topic was created or not.
         */
        public boolean createTopicWithReplicasAssignments(String name, Map<Integer, List<Integer>> replicaAssignments,
                                                          Map<String, String> configs, CreateTopicsOptions options) {
            NewTopic topic = new NewTopic(name, replicaAssignments).configs(configs);
            return createTopics(Collections.singletonList(topic), options);
        }

        /**
         * List the topics available in the cluster.
         *
         * @param excludeInternalTopics If exclude internal topics in the returned list.
         * @return A list with topic names.
         */
        public List<String> getTopics(boolean excludeInternalTopics) {
            return toJavaList(topics.getTopics(scalaOption(null), excludeInternalTopics));
        }

        /**
         * List the topics available in the cluster.
         *
         * @param topicWhitelist        Whitelist to filter the available topics.
         * @param excludeInternalTopics If exclude internal topics in the returned list.
         * @return A list with topic names.
         */
        public List<String> getTopics(String topicWhitelist, boolean excludeInternalTopics) {
            return toJavaList(topics.getTopics(scalaOption(topicWhitelist), excludeInternalTopics));
        }

        /**
         * Print the topics available in the cluster.
         *
         * @param excludeInternalTopics If exclude internal topics in the returned list.
         */
        public void listTopics(boolean excludeInternalTopics) {
            topics.listTopics(scalaOption(null), excludeInternalTopics);
        }

        /**
         * Print the topics available in the cluster.
         *
         * @param topicWhitelist        Whitelist to filter the available topics.
         * @param excludeInternalTopics If exclude internal topics in the returned list.
         */
        public void listTopics(String topicWhitelist, boolean excludeInternalTopics) {
            topics.listTopics(scalaOption(topicWhitelist), excludeInternalTopics);
        }

        /**
         * Print the partition description related for a topic.
         *
         * @param name           Topic name.
         * @param withConfigs    If including config info.
         * @param withPartitions If including partitions info.
         */
        public void describeTopic(String name, boolean withConfigs, boolean withPartitions) {
            describeTopics(Collections.singletonList(name), new DescribeTopicsOptions(), withConfigs, withPartitions);
        }

        /**
         * Print the partition description related for a topic.
         *
         * @param topics         Topic list.
         * @param options        Extra describe topics options.
         * @param withConfigs    If including config info.
         * @param withPartitions If including partitions info.
         */
        public void describeTopics(List<String> topics, DescribeTopicsOptions options,
                                   boolean withConfigs, boolean withPartitions) {
            this.topics.describeTopics(toScalaSeq(topics), scalaOption(options), withConfigs, withPartitions);
        }

        /**
         * Delete a topic in the cluster.
         *
         * @param name Topic name.
         */
        public void deleteTopic(String name) {
            deleteTopics(Collections.singletonList(name));
        }

        /**
         * Delete topics in the cluster.
         *
         * @param topics Topic list.
         */
        public void deleteTopics(List<String> topics) {
            this.topics.deleteTopics(toScalaSeq(topics));
        }

        /**
         * Get the new partition distribution for a topic (does not modify anything in the cluster).
         *
         * @param topic             Topic name.
         * @param partitions        Number of partitions.
         * @param replicaAssignment A {@code Map[Integer, List[Integer]]} from partition id to replica ids.
         * @return A {@code Map[String, NewPartitions]} with the topic and its new partitions.
         */
        public Map<String, NewPartitions> getNewPartitionsDistribution(String topic, int partitions,
                                                                       Map<Integer, List<Integer>> replicaAssignment) {
            return getNewPartitionsDistribution(Collections.singletonList(topic), partitions, replicaAssignment);
        }

        /**
         * Get the new partition distribution for a topic (does not modify anything in the cluster).
         *
         * @param topics            Topic list.
         * @param partitions        Number of partitions.
         * @param replicaAssignment A {@code Map[Integer, List[Integer]]} from partition id to replica ids.
         * @return A {@code Map[String, NewPartitions]} with the topic and its new partitions.
         */
        @SuppressWarnings("unchecked")
        public Map<String, NewPartitions> getNewPartitionsDistribution(List<String> topics, int partitions,
                                                                       Map<Integer, List<Integer>> replicaAssignment) {
            Object assignments = toScalaMap(
                    replicaAssignment.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, entry -> toScalaSeq(entry.getValue())))
            );
            return toJavaMap(
                    this.topics.getNewPartitionsDistribution(
                            toScalaSeq(topics),
                            partitions,
                            (scala.collection.immutable.Map<Object, scala.collection.Seq<Object>>) assignments
                    )
            );
        }

        /**
         * Modify the new partition distribution for a topic.
         *
         * @param topic             Topic name.
         * @param partitions        Number of partitions.
         * @param replicaAssignment A {@code Map[Integer, List[Integer]]} from partition id to replica ids.
         */
        public void alterTopic(String topic, int partitions, Map<Integer, List<Integer>> replicaAssignment) {
            alterTopics(Collections.singletonList(topic), partitions, replicaAssignment);
        }

        /**
         * Modify the new partition distribution for a topic list.
         *
         * @param topics            Topic list.
         * @param partitions        Number of partitions.
         * @param replicaAssignment A {@code Map[Integer, List[Integer]]} from partition id to replica ids.
         */
        @SuppressWarnings("unchecked")
        public void alterTopics(List<String> topics, int partitions, Map<Integer, List<Integer>> replicaAssignment) {
            Object assignments = toScalaMap(
                    replicaAssignment.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, entry -> toScalaSeq(entry.getValue())))
            );
            this.topics.alterTopics(
                    toScalaSeq(topics),
                    partitions,
                    (scala.collection.immutable.Map<Object, scala.collection.Seq<Object>>) assignments
            );
        }
    }

    /**
     * Class which includes all administrative functionalities related with configurations.
     */
    public static class KAdminConfigs {

        private final com.github.mmolimar.kukulcan.KAdmin.KAdminConfigs configs;

        public KAdminConfigs(com.github.mmolimar.kukulcan.KAdmin.KAdminConfigs configs) {
            this.configs = configs;
        }

        /**
         * Modify a topic configuration.
         * Updates are not transactional so they may succeed for some resources while fail for others. The configs for
         * a particular resource are updated atomically.
         *
         * @param name               Topic name.
         * @param configsToBeAdded   {@code Map[String, String]} with the configs to add.
         * @param configsToBeDeleted {@code List[String]} with the configs to delete.
         */
        public void alterTopicConfig(String name, Map<String, String> configsToBeAdded,
                                     List<String> configsToBeDeleted) {
            configs.alterTopicConfig(name, toScalaMap(configsToBeAdded), toScalaSeq(configsToBeDeleted));
        }

        /**
         * Modify a broker configuration.
         * Updates are not transactional so they may succeed for some resources while fail for others. The configs for
         * a particular resource are updated atomically.
         *
         * @param brokerId           Broker to update.
         * @param configsToBeAdded   {@code Map[String, String]} with the configs to add.
         * @param configsToBeDeleted {@code List[String]} with the configs to delete.
         */
        public void alterBrokerConfig(int brokerId, Map<String, String> configsToBeAdded,
                                      List<String> configsToBeDeleted) {
            configs.alterBrokerConfig(scalaOption(brokerId), toScalaMap(configsToBeAdded),
                    toScalaSeq(configsToBeDeleted));
        }

        /**
         * Modify a brokers configuration.
         * Updates are not transactional so they may succeed for some resources while fail for others. The configs for
         * a particular resource are updated atomically.
         *
         * @param configsToBeAdded   {@code Map[String, String]} with the configs to add.
         * @param configsToBeDeleted {@code List[String]} with the configs to delete.
         */
        public void alterBrokerConfig(Map<String, String> configsToBeAdded, List<String> configsToBeDeleted) {
            configs.alterBrokerConfig(scalaOption(null), toScalaMap(configsToBeAdded), toScalaSeq(configsToBeDeleted));
        }

        /**
         * Modify a broker logger configuration.
         * Updates are not transactional so they may succeed for some resources while fail for others. The configs for
         * a particular resource are updated atomically.
         *
         * @param brokerId           Broker to update.
         * @param configsToBeAdded   {@code Map[String, String]} with the configs to add.
         * @param configsToBeDeleted {@code List[String]} with the configs to delete.
         */
        public void alterBrokerLoggerConfig(int brokerId, Map<String, String> configsToBeAdded,
                                            List<String> configsToBeDeleted) {
            configs.alterBrokerLoggerConfig(scalaOption(brokerId), toScalaMap(configsToBeAdded),
                    toScalaSeq(configsToBeDeleted));
        }

        /**
         * Modify a brokers logger configuration.
         * Updates are not transactional so they may succeed for some resources while fail for others. The configs for
         * a particular resource are updated atomically.
         *
         * @param configsToBeAdded   {@code Map[String, String]} with the configs to add.
         * @param configsToBeDeleted {@code List[String]} with the configs to delete.
         */
        public void alterBrokerLoggerConfig(Map<String, String> configsToBeAdded, List<String> configsToBeDeleted) {
            configs.alterBrokerLoggerConfig(scalaOption(null), toScalaMap(configsToBeAdded),
                    toScalaSeq(configsToBeDeleted));
        }

        /**
         * Get each config entry for the topic.
         *
         * @param name        Topic to describe.
         * @param describeAll If describe all configs.
         * @return A list with all config entries.
         */
        public List<ConfigEntry> getDescribeTopicConfig(String name, boolean describeAll) {
            return toJavaList(configs.getDescribeTopicConfig(scalaOption(name), describeAll));
        }

        /**
         * Get each config entry for the topics.
         *
         * @param describeAll If describe all configs.
         * @return A list with all config entries.
         */
        public List<ConfigEntry> getDescribeTopicConfig(boolean describeAll) {
            return toJavaList(configs.getDescribeTopicConfig(scalaOption(null), describeAll));
        }

        /**
         * Get each config entry for the broker.
         *
         * @param brokerId    Broker id to describe.
         * @param describeAll If describe all configs.
         * @return A list with all config entries.
         */
        public List<ConfigEntry> getDescribeBrokerConfig(int brokerId, boolean describeAll) {
            return toJavaList(configs.getDescribeBrokerConfig(scalaOption(brokerId), describeAll));
        }

        /**
         * Get each config entry for the brokers.
         *
         * @param describeAll If describe all configs.
         * @return A list with all config entries.
         */
        public List<ConfigEntry> getDescribeBrokerConfig(boolean describeAll) {
            return toJavaList(configs.getDescribeBrokerConfig(scalaOption(null), describeAll));
        }

        /**
         * Get each config entry for the broker(s) logger.
         *
         * @param brokerId    Broker id to describe.
         * @param describeAll If describe all configs.
         * @return A list with all config entries.
         */
        public List<ConfigEntry> getDescribeBrokerLoggerConfig(int brokerId, boolean describeAll) {
            return toJavaList(configs.getDescribeBrokerLoggerConfig(scalaOption(brokerId), describeAll));
        }

        /**
         * Get each config entry for the brokers logger.
         *
         * @param describeAll If describe all configs.
         * @return A list with all config entries.
         */
        public List<ConfigEntry> getDescribeBrokerLoggerConfig(boolean describeAll) {
            return toJavaList(configs.getDescribeBrokerLoggerConfig(scalaOption(null), describeAll));
        }

        /**
         * Print each config entry for the topic.
         *
         * @param name        Topic to describe.
         * @param describeAll If describe all configs.
         * @return A list with all config entries.
         */
        public void describeTopicConfig(String name, boolean describeAll) {
            configs.describeTopicConfig(scalaOption(name), describeAll);
        }

        /**
         * Print each config entry for the topics.
         *
         * @param describeAll If describe all configs.
         * @return A list with all config entries.
         */
        public void describeTopicConfig(boolean describeAll) {
            configs.describeTopicConfig(scalaOption(null), describeAll);
        }

        /**
         * Print each config entry for the broker.
         *
         * @param brokerId    Broker id to describe.
         * @param describeAll If describe all configs.
         * @return A list with all config entries.
         */
        public void describeBrokerConfig(int brokerId, boolean describeAll) {
            configs.describeBrokerConfig(scalaOption(brokerId), describeAll);
        }

        /**
         * Print each config entry for the brokers.
         *
         * @param describeAll If describe all configs.
         * @return A list with all config entries.
         */
        public void describeBrokerConfig(boolean describeAll) {
            configs.describeBrokerConfig(scalaOption(null), describeAll);
        }

        /**
         * Print each config entry for the broker logger.
         *
         * @param brokerId    Broker id to describe.
         * @param describeAll If describe all configs.
         * @return A list with all config entries.
         */
        public void describeBrokerLoggerConfig(int brokerId, boolean describeAll) {
            configs.describeBrokerConfig(scalaOption(brokerId), describeAll);
        }

        /**
         * Print each config entry for the brokers logger.
         *
         * @param describeAll If describe all configs.
         * @return A list with all config entries.
         */
        public void describeBrokerLoggerConfig(boolean describeAll) {
            configs.describeBrokerConfig(scalaOption(null), describeAll);
        }

    }

    /**
     * Class which includes all administrative functionalities related with ACLs.
     */
    public static class KAdminAcls {

        private final com.github.mmolimar.kukulcan.KAdmin.KAdminAcls acls;

        KAdminAcls(com.github.mmolimar.kukulcan.KAdmin.KAdminAcls acls) {
            this.acls = acls;
        }

        /**
         * Create a default Kafka Authorizer.
         *
         * @return An {@code Optional[Authorizer]} authorizer for the Kafka brokers.
         */
        public Optional<Authorizer> defaultAuthorizer() {
            return toJavaOption(acls.defaultAuthorizer());
        }

        /**
         * Adds ACLs in the Kafka cluster.
         * This operation is not transactional so it may succeed for some ACLs while fail for others.
         *
         * @param aclBindings A list with the binding between a resource pattern and an access control entry.
         */
        public void addAcls(List<AclBinding> aclBindings) {
            addAcls(aclBindings, new CreateAclsOptions());
        }

        /**
         * Adds ACLs in the Kafka cluster.
         * This operation is not transactional so it may succeed for some ACLs while fail for others.
         *
         * @param aclBindings A list with the binding between a resource pattern and an access control entry.
         * @param options     Extra configuration options for creating the ACLs.
         */
        public void addAcls(List<AclBinding> aclBindings, CreateAclsOptions options) {
            addAcls(aclBindings, options, true);
        }

        /**
         * Adds ACLs in the Kafka cluster.
         * This operation is not transactional so it may succeed for some ACLs while fail for others.
         *
         * @param aclBindings     A list with the binding between a resource pattern and an access control entry.
         * @param forceAuthorizer If force the use of an authorizer.
         */
        public void addAcls(List<AclBinding> aclBindings, boolean forceAuthorizer) {
            addAcls(aclBindings, new CreateAclsOptions(), forceAuthorizer);
        }

        /**
         * Adds ACLs in the Kafka cluster.
         * This operation is not transactional so it may succeed for some ACLs while fail for others.
         *
         * @param aclBindings     A list with the binding between a resource pattern and an access control entry.
         * @param options         Extra configuration options for creating the ACLs.
         * @param forceAuthorizer If force the use of an authorizer.
         */
        public void addAcls(List<AclBinding> aclBindings, CreateAclsOptions options, boolean forceAuthorizer) {
            acls.addAcls(toScalaSeq(aclBindings), options, forceAuthorizer);
        }

        /**
         * Remove ACLs in the Kafka cluster.
         * This operation is not transactional so it may succeed for some ACLs while fail for others.
         *
         * @param filters A list with the filters which can match AclBinding objects.
         */
        public void removeAcls(List<AclBindingFilter> filters) {
            removeAcls(filters, new DeleteAclsOptions());
        }

        /**
         * Remove ACLs in the Kafka cluster.
         * This operation is not transactional so it may succeed for some ACLs while fail for others.
         *
         * @param filters A list with the filters which can match AclBinding objects.
         * @param options Extra configuration options for deleting the ACLs.
         */
        public void removeAcls(List<AclBindingFilter> filters, DeleteAclsOptions options) {
            removeAcls(filters, options, true);
        }

        /**
         * Remove ACLs in the Kafka cluster.
         * This operation is not transactional so it may succeed for some ACLs while fail for others.
         *
         * @param filters         A list with the filters which can match AclBinding objects.
         * @param forceAuthorizer If force the use of an authorizer.
         */
        public void removeAcls(List<AclBindingFilter> filters, boolean forceAuthorizer) {
            removeAcls(filters, new DeleteAclsOptions(), forceAuthorizer);
        }

        /**
         * Remove ACLs in the Kafka cluster.
         * This operation is not transactional so it may succeed for some ACLs while fail for others.
         *
         * @param filters         A list with the filters which can match AclBinding objects.
         * @param options         Extra configuration options for deleting the ACLs.
         * @param forceAuthorizer If force the use of an authorizer.
         */
        public void removeAcls(List<AclBindingFilter> filters, DeleteAclsOptions options, boolean forceAuthorizer) {
            acls.removeAcls(toScalaSeq(filters), options, forceAuthorizer);
        }

        /**
         * Get ACLs in the Kafka cluster.
         *
         * @return The result with the all ACLs.
         */
        public List<AclBinding> getAcls() {
            return getAcls(AclBindingFilter.ANY, new DescribeAclsOptions());
        }

        /**
         * Get ACLs in the Kafka cluster.
         *
         * @param filter  An optional filter which can match AclBinding objects.
         * @param options Extra configuration options for describing the ACLs.
         * @return The result with the all ACLs.
         */
        public List<AclBinding> getAcls(AclBindingFilter filter, DescribeAclsOptions options) {
            return getAcls(filter, options, true);
        }

        /**
         * Get ACLs in the Kafka cluster.
         *
         * @param filter          An optional filter which can match AclBinding objects.
         * @param forceAuthorizer If force the use of an authorizer.
         * @return The result with the all ACLs.
         */
        public List<AclBinding> getAcls(AclBindingFilter filter, boolean forceAuthorizer) {
            return getAcls(filter, new DescribeAclsOptions(), forceAuthorizer);
        }

        /**
         * Get ACLs in the Kafka cluster.
         *
         * @param filter          An optional filter which can match AclBinding objects.
         * @param options         Extra configuration options for describing the ACLs.
         * @param forceAuthorizer If force the use of an authorizer.
         * @return The result with the all ACLs.
         */
        public List<AclBinding> getAcls(AclBindingFilter filter, DescribeAclsOptions options, boolean forceAuthorizer) {
            return toJavaList(acls.getAcls(filter, options, forceAuthorizer));
        }

        /**
         * Print ACLs in the Kafka cluster.
         */
        public void listAcls() {
            listAcls(AclBindingFilter.ANY, new DescribeAclsOptions());
        }

        /**
         * Print ACLs in the Kafka cluster.
         *
         * @param filter  An optional filter which can match AclBinding objects.
         * @param options Extra configuration options for describing the ACLs.
         */
        public void listAcls(AclBindingFilter filter, DescribeAclsOptions options) {
            listAcls(filter, Collections.emptyList(), options);
        }

        /**
         * Print ACLs in the Kafka cluster.
         *
         * @param filter     An optional filter which can match AclBinding objects.
         * @param principals Principals associated with each ACL. If empty, the list will not be grouped.
         */
        public void listAcls(AclBindingFilter filter, List<KafkaPrincipal> principals) {
            listAcls(filter, principals, new DescribeAclsOptions());
        }

        /**
         * Print ACLs in the Kafka cluster.
         *
         * @param filter     An optional filter which can match AclBinding objects.
         * @param principals Principals associated with each ACL. If empty, the list will not be grouped.
         * @param options    Extra configuration options for describing the ACLs.
         */
        public void listAcls(AclBindingFilter filter, List<KafkaPrincipal> principals, DescribeAclsOptions options) {
            acls.listAcls(filter, toScalaSeq(principals), options);
        }
    }

    /**
     * Class which includes functionalities related to see Kafka metrics.
     */
    public static class KAdminMetrics {

        private final com.github.mmolimar.kukulcan.KAdmin.KAdminMetrics metrics;

        KAdminMetrics(com.github.mmolimar.kukulcan.KAdmin.KAdminMetrics metrics) {
            this.metrics = metrics;
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
         * @return a {@code Map[MetricName, Metric]} with the all metrics registered filtered by the group and name
         * regular expressions.
         */
        public Map<MetricName, Metric> getMetrics(String groupRegex, String nameRegex) {
            return toJavaMap(metrics.getMetrics(groupRegex, nameRegex));
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
            metrics.listMetrics(groupRegex, nameRegex);
        }

    }
}
