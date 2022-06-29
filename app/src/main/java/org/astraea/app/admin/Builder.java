/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.admin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.MemberToRemove;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ElectionNotNeededException;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.astraea.app.common.Utils;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.cost.NodeInfo;
import org.astraea.app.cost.ReplicaInfo;

public class Builder {

  private final Map<String, Object> configs = new HashMap<>();

  Builder() {}

  public Builder bootstrapServers(String bootstrapServers) {
    this.configs.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(bootstrapServers));
    return this;
  }

  public Builder configs(Map<String, String> configs) {
    this.configs.putAll(configs);
    return this;
  }

  public Admin build() {
    return new AdminImpl(org.apache.kafka.clients.admin.Admin.create(configs));
  }

  private static class AdminImpl implements Admin {
    private final org.apache.kafka.clients.admin.Admin admin;

    AdminImpl(org.apache.kafka.clients.admin.Admin admin) {
      this.admin = Objects.requireNonNull(admin);
    }

    @Override
    public void close() {
      admin.close();
    }

    @Override
    public Set<NodeInfo> nodes() {
      return Utils.packException(
          () ->
              admin.describeCluster().nodes().get().stream()
                  .map(NodeInfo::of)
                  .collect(Collectors.toUnmodifiableSet()));
    }

    @Override
    public Map<Integer, Set<String>> brokerFolders(Set<Integer> brokers) {
      return Utils.packException(
          () ->
              admin.describeLogDirs(brokers).allDescriptions().get().entrySet().stream()
                  .collect(Collectors.toMap(Map.Entry::getKey, map -> map.getValue().keySet())));
    }

    @Override
    public ReplicaMigrator migrator() {
      return new MigratorImpl(admin, this::partitions);
    }

    @Override
    public void preferredLeaderElection(TopicPartition topicPartition) {
      try {
        Utils.packException(
            () -> {
              admin
                  .electLeaders(ElectionType.PREFERRED, Set.of(TopicPartition.to(topicPartition)))
                  .all()
                  .get();
            });
      } catch (ElectionNotNeededException ignored) {
        // Swallow the ElectionNotNeededException.
        // This error occurred if the preferred leader of the given topic/partition is already the
        // leader. It is ok to swallow the exception since the preferred leader be the actual
        // leader. That is what the caller wants to be.
      }
    }

    @Override
    public Map<TopicPartition, Collection<ProducerState>> producerStates(
        Set<TopicPartition> partitions) {
      return Utils.packException(
          () ->
              admin
                  .describeProducers(
                      partitions.stream()
                          .map(TopicPartition::to)
                          .collect(Collectors.toUnmodifiableList()))
                  .all()
                  .get()
                  .entrySet()
                  .stream()
                  .filter(e -> !e.getValue().activeProducers().isEmpty())
                  .collect(
                      Collectors.toMap(
                          e -> TopicPartition.from(e.getKey()),
                          e ->
                              e.getValue().activeProducers().stream()
                                  .map(ProducerState::from)
                                  .collect(Collectors.toUnmodifiableList()))));
    }

    @Override
    public Set<String> consumerGroupIds() {
      return Utils.packException(() -> admin.listConsumerGroups().all().get()).stream()
          .map(ConsumerGroupListing::groupId)
          .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Map<String, ConsumerGroup> consumerGroups(Set<String> consumerGroupNames) {
      return Utils.packException(
          () -> {
            var consumerGroupDescriptions =
                admin.describeConsumerGroups(consumerGroupNames).all().get();

            var consumerGroupMetadata =
                consumerGroupNames.stream()
                    .map(x -> Map.entry(x, admin.listConsumerGroupOffsets(x)))
                    .map(
                        x ->
                            Map.entry(
                                x.getKey(),
                                Utils.packException(
                                    () -> x.getValue().partitionsToOffsetAndMetadata().get())))
                    .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

            var createMember =
                (BiFunction<String, MemberDescription, Member>)
                    (s, x) ->
                        new Member(s, x.consumerId(), x.groupInstanceId(), x.clientId(), x.host());

            return consumerGroupNames.stream()
                .map(
                    groupId -> {
                      var members =
                          consumerGroupDescriptions.get(groupId).members().stream()
                              .map(x -> createMember.apply(groupId, x))
                              .collect(Collectors.toUnmodifiableList());
                      var consumeOffset =
                          consumerGroupMetadata.get(groupId).entrySet().stream()
                              .collect(
                                  Collectors.toUnmodifiableMap(
                                      tp -> TopicPartition.from(tp.getKey()),
                                      x -> x.getValue().offset()));
                      var assignment =
                          consumerGroupDescriptions.get(groupId).members().stream()
                              .collect(
                                  Collectors.toUnmodifiableMap(
                                      x -> createMember.apply(groupId, x),
                                      x ->
                                          x.assignment().topicPartitions().stream()
                                              .map(TopicPartition::from)
                                              .collect(Collectors.toSet())));

                      return Map.entry(
                          groupId, new ConsumerGroup(groupId, members, consumeOffset, assignment));
                    })
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
          });
    }

    private Map<TopicPartition, Long> earliestOffset(Set<TopicPartition> partitions) {

      return Utils.packException(
          () ->
              admin
                  .listOffsets(
                      partitions.stream()
                          .collect(
                              Collectors.toMap(
                                  TopicPartition::to, e -> new OffsetSpec.EarliestSpec())))
                  .all()
                  .get()
                  .entrySet()
                  .stream()
                  .collect(
                      Collectors.toMap(
                          e -> TopicPartition.from(e.getKey()), e -> e.getValue().offset())));
    }

    private Map<TopicPartition, Long> latestOffset(Set<TopicPartition> partitions) {
      return Utils.packException(
          () ->
              admin
                  .listOffsets(
                      partitions.stream()
                          .collect(
                              Collectors.toMap(
                                  TopicPartition::to, e -> new OffsetSpec.LatestSpec())))
                  .all()
                  .get()
                  .entrySet()
                  .stream()
                  .collect(
                      Collectors.toMap(
                          e -> TopicPartition.from(e.getKey()), e -> e.getValue().offset())));
    }

    @Override
    public Map<String, Config> topics(Set<String> topicNames) {
      return Utils.packException(
              () ->
                  admin
                      .describeConfigs(
                          topicNames.stream()
                              .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
                              .collect(Collectors.toList()))
                      .all()
                      .get())
          .entrySet()
          .stream()
          .collect(Collectors.toMap(e -> e.getKey().name(), e -> new ConfigImpl(e.getValue())));
    }

    @Override
    public Set<String> topicNames() {
      return Utils.packException(
          () -> admin.listTopics(new ListTopicsOptions().listInternal(true)).names().get());
    }

    @Override
    public Map<Integer, Config> brokers(Set<Integer> brokerIds) {
      return Utils.packException(
              () ->
                  admin
                      .describeConfigs(
                          brokerIds.stream()
                              .map(
                                  id ->
                                      new ConfigResource(
                                          ConfigResource.Type.BROKER, String.valueOf(id)))
                              .collect(Collectors.toList()))
                      .all()
                      .get())
          .entrySet()
          .stream()
          .collect(
              Collectors.toMap(
                  e -> Integer.valueOf(e.getKey().name()), e -> new ConfigImpl(e.getValue())));
    }

    @Override
    public Map<TopicPartition, Offset> offsets(Set<String> topics) {
      var partitions = partitions(topics);
      var earliest = earliestOffset(partitions);
      var latest = latestOffset(partitions);
      return earliest.entrySet().stream()
          .filter(e -> latest.containsKey(e.getKey()))
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey, e -> new Offset(e.getValue(), latest.get(e.getKey()))));
    }

    @Override
    public Set<TopicPartition> partitions(Set<String> topics) {
      return Utils.packException(
          () ->
              admin.describeTopics(topics).all().get().entrySet().stream()
                  .flatMap(
                      e ->
                          e.getValue().partitions().stream()
                              .map(p -> new TopicPartition(e.getKey(), p.partition())))
                  .collect(Collectors.toSet()));
    }

    @Override
    public Map<Integer, Set<TopicPartition>> partitions(
        Set<String> topics, Set<Integer> brokerIds) {
      return replicas(topics).entrySet().stream()
          .flatMap(
              e -> e.getValue().stream().map(replica -> Map.entry(replica.broker(), e.getKey())))
          .filter(e -> brokerIds.contains(e.getKey()))
          .collect(Collectors.groupingBy(Map.Entry::getKey))
          .entrySet()
          .stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey,
                  e -> e.getValue().stream().map(Map.Entry::getValue).collect(Collectors.toSet())));
    }

    @Override
    public Map<TopicPartition, List<Replica>> replicas(Set<String> topics) {
      return Utils.packException(
          () -> {
            var logInfo = admin.describeLogDirs(brokerIds()).allDescriptions().get();
            var tpPathMap = new HashMap<TopicPartition, Map<Integer, String>>();
            for (var entry0 : logInfo.entrySet()) {
              for (var entry1 : entry0.getValue().entrySet()) {
                for (var entry2 : entry1.getValue().replicaInfos().entrySet()) {
                  var tp = TopicPartition.from(entry2.getKey());
                  var broker = entry0.getKey();
                  var dataPath = entry1.getKey();
                  tpPathMap.computeIfAbsent(tp, (ignore) -> new HashMap<>()).put(broker, dataPath);
                }
              }
            }
            return admin.describeTopics(topics).allTopicNames().get().entrySet().stream()
                .flatMap(
                    entry ->
                        entry.getValue().partitions().stream()
                            .map(
                                tpInfo -> {
                                  var topicName = entry.getKey();
                                  var partition = tpInfo.partition();
                                  var topicPartition = new TopicPartition(topicName, partition);
                                  return Map.entry(topicPartition, tpInfo);
                                }))
                .collect(
                    Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        (entry) -> {
                          var topicPartition = entry.getKey();
                          var tpInfo = entry.getValue();
                          var replicaLeaderId = tpInfo.leader() != null ? tpInfo.leader().id() : -1;
                          var isrSet = tpInfo.isr();
                          // The first replica in the return result is the preferred leader. This
                          // only works with Kafka broker version after 0.11. Version before 0.11
                          // returns the replicas in unspecified order due to a bug.
                          var preferredLeader = entry.getValue().replicas().get(0);
                          return entry.getValue().replicas().stream()
                              .map(
                                  node -> {
                                    int broker = node.id();
                                    var dataPath =
                                        tpPathMap
                                            .getOrDefault(topicPartition, Map.of())
                                            .getOrDefault(broker, null);
                                    var replicaInfo =
                                        node.isEmpty() || dataPath == null
                                            ? null
                                            : logInfo
                                                .get(broker)
                                                .get(dataPath)
                                                .replicaInfos()
                                                .get(TopicPartition.to(topicPartition));
                                    boolean isLeader = node.id() == replicaLeaderId;
                                    boolean inSync = isrSet.contains(node);
                                    long lag = replicaInfo != null ? replicaInfo.offsetLag() : -1L;
                                    long size = replicaInfo != null ? replicaInfo.size() : -1L;
                                    boolean future = replicaInfo != null && replicaInfo.isFuture();
                                    boolean offline = node.isEmpty();
                                    boolean isPreferredLeader = preferredLeader.id() == broker;
                                    return new Replica(
                                        broker,
                                        lag,
                                        size,
                                        isLeader,
                                        inSync,
                                        future,
                                        offline,
                                        isPreferredLeader,
                                        dataPath);
                                  })
                              .collect(Collectors.toList());
                        }));
          });
    }

    @Override
    public TopicCreator creator() {
      return new CreatorImpl(
          admin, topic -> this.replicas(Set.of(topic)), topic -> topics().get(topic));
    }

    @Override
    public QuotaCreator quotaCreator() {
      return new QuotaImpl(admin);
    }

    @Override
    public Collection<Quota> quotas(Quota.Target target) {
      return quotas(
          ClientQuotaFilter.contains(
              List.of(ClientQuotaFilterComponent.ofEntityType(target.nameOfKafka()))));
    }

    @Override
    public Collection<Quota> quotas(Quota.Target target, String value) {
      return quotas(
          ClientQuotaFilter.contains(
              List.of(ClientQuotaFilterComponent.ofEntity(target.nameOfKafka(), value))));
    }

    @Override
    public Collection<Quota> quotas() {
      return quotas(ClientQuotaFilter.all());
    }

    private Collection<Quota> quotas(ClientQuotaFilter filter) {
      return Quota.of(
          Utils.packException(() -> admin.describeClientQuotas(filter).entities().get()));
    }

    @Override
    public ClusterInfo clusterInfo(Set<String> topics) {
      final var nodeInfo = this.nodes().stream().collect(Collectors.toUnmodifiableList());

      final Map<String, List<ReplicaInfo>> topicToReplicasMap =
          Utils.packException(() -> this.replicas(topics)).entrySet().stream()
              .flatMap(
                  entry -> {
                    final var topicPartition = entry.getKey();
                    final var replicas = entry.getValue();

                    return replicas.stream()
                        .map(
                            replica ->
                                ReplicaInfo.of(
                                    topicPartition.topic(),
                                    topicPartition.partition(),
                                    nodeInfo.stream()
                                        .filter(x -> x.id() == replica.broker())
                                        .findFirst()
                                        .orElse(NodeInfo.ofOfflineNode(replica.broker())),
                                    replica.leader(),
                                    replica.inSync(),
                                    replica.isOffline(),
                                    replica.path()));
                  })
              .collect(Collectors.groupingBy(ReplicaInfo::topic));
      final var dataDirectories =
          this.brokerFolders(
                  nodeInfo.stream().map(NodeInfo::id).collect(Collectors.toUnmodifiableSet()))
              .entrySet()
              .stream()
              .collect(
                  Collectors.toUnmodifiableMap(Map.Entry::getKey, x -> Set.copyOf(x.getValue())));

      return new ClusterInfo() {
        @Override
        public List<NodeInfo> nodes() {
          return nodeInfo;
        }

        @Override
        public Set<String> dataDirectories(int brokerId) {
          final var set = dataDirectories.get(brokerId);
          if (set == null) throw new NoSuchElementException("Unknown broker \"" + brokerId + "\"");
          else return set;
        }

        @Override
        public List<ReplicaInfo> availableReplicaLeaders(String topic) {
          return replicas(topic).stream()
              .filter(ReplicaInfo::isLeader)
              .filter(ReplicaInfo::isOnlineReplica)
              .collect(Collectors.toUnmodifiableList());
        }

        @Override
        public List<ReplicaInfo> availableReplicas(String topic) {
          return replicas(topic).stream()
              .filter(ReplicaInfo::isOnlineReplica)
              .collect(Collectors.toUnmodifiableList());
        }

        @Override
        public Set<String> topics() {
          return topics;
        }

        @Override
        public List<ReplicaInfo> replicas(String topic) {
          final var replicaInfos = topicToReplicasMap.get(topic);
          if (replicaInfos == null)
            throw new NoSuchElementException(
                "This ClusterInfo have no information about topic \"" + topic + "\"");
          else return replicaInfos;
        }

        @Override
        public ClusterBean clusterBean() {
          return ClusterBean.of(Map.of());
        }
      };
    }

    @Override
    public Set<String> transactionIds() {
      return Utils.packException(
          () ->
              admin.listTransactions().all().get().stream()
                  .map(TransactionListing::transactionalId)
                  .collect(Collectors.toUnmodifiableSet()));
    }

    @Override
    public Map<String, Transaction> transactions(Set<String> transactionIds) {
      return Utils.packException(
          () ->
              admin.describeTransactions(transactionIds).all().get().entrySet().stream()
                  .collect(
                      Collectors.toMap(Map.Entry::getKey, e -> Transaction.from(e.getValue()))));
    }

    @Override
    public void removeGroup(String groupId) {
      Utils.packException(() -> admin.deleteConsumerGroups(Set.of(groupId)).all().get());
    }

    @Override
    public void removeAllMembers(String groupId) {
      Utils.packException(
          () ->
              admin
                  .removeMembersFromConsumerGroup(
                      groupId, new RemoveMembersFromConsumerGroupOptions())
                  .all()
                  .get());
    }

    @Override
    public void removeStaticMembers(String groupId, Set<String> members) {
      Utils.packException(
          () ->
              admin
                  .removeMembersFromConsumerGroup(
                      groupId,
                      new RemoveMembersFromConsumerGroupOptions(
                          members.stream()
                              .map(MemberToRemove::new)
                              .collect(Collectors.toUnmodifiableList())))
                  .all()
                  .get());
    }

    @Override
    public Map<TopicPartition, Reassignment> reassignments(Set<String> topics) {
      var assignments =
          Utils.packException(
              () ->
                  admin
                      .listPartitionReassignments(
                          partitions(topics).stream()
                              .map(TopicPartition::to)
                              .collect(Collectors.toSet()))
                      .reassignments()
                      .get());

      var dirs =
          Utils.packException(
              () ->
                  admin
                      .describeReplicaLogDirs(
                          replicas(topics).entrySet().stream()
                              .flatMap(
                                  e ->
                                      e.getValue().stream()
                                          .map(
                                              r ->
                                                  TopicPartitionReplica.to(
                                                      new TopicPartitionReplica(
                                                          e.getKey().topic(),
                                                          e.getKey().partition(),
                                                          r.broker()))))
                              .collect(Collectors.toUnmodifiableList()))
                      .all()
                      .get());

      var result =
          new HashMap<
              TopicPartition, Map.Entry<Set<Reassignment.Location>, Set<Reassignment.Location>>>();

      dirs.forEach(
          (replica, logDir) -> {
            var brokerId = replica.brokerId();
            var tp = new TopicPartition(replica.topic(), replica.partition());
            var ls =
                result.computeIfAbsent(tp, ignored -> Map.entry(new HashSet<>(), new HashSet<>()));
            // the replica is moved from a folder to another folder (in the same node)
            if (logDir.getFutureReplicaLogDir() != null)
              ls.getValue()
                  .add(new Reassignment.Location(brokerId, logDir.getFutureReplicaLogDir()));
            if (logDir.getCurrentReplicaLogDir() != null) {
              var assignment = assignments.get(TopicPartition.to(tp));
              // the replica is moved from a node to another node
              if (assignment != null && assignment.addingReplicas().contains(brokerId)) {
                ls.getValue()
                    .add(new Reassignment.Location(brokerId, logDir.getCurrentReplicaLogDir()));
              } else {
                ls.getKey()
                    .add(new Reassignment.Location(brokerId, logDir.getCurrentReplicaLogDir()));
              }
            }
          });

      return result.entrySet().stream()
          // empty "to" means there is no reassignment
          .filter(e -> !e.getValue().getValue().isEmpty())
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey,
                  e ->
                      new Reassignment(
                          Collections.unmodifiableSet(e.getValue().getKey()),
                          Collections.unmodifiableSet(e.getValue().getValue()))));
    }
  }

  private static class ConfigImpl implements Config {
    private final Map<String, String> configs;

    ConfigImpl(org.apache.kafka.clients.admin.Config config) {
      this(
          config.entries().stream()
              .filter(e -> e.value() != null)
              .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value)));
    }

    ConfigImpl(Map<String, String> configs) {
      this.configs = Collections.unmodifiableMap(configs);
    }

    @Override
    public Optional<String> value(String key) {
      return Optional.ofNullable(configs.get(key));
    }

    @Override
    public Set<String> keys() {
      return configs.keySet();
    }

    @Override
    public Collection<String> values() {
      return configs.values();
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
      return configs.entrySet().iterator();
    }
  }

  private static class CreatorImpl implements TopicCreator {
    private final org.apache.kafka.clients.admin.Admin admin;
    private final Function<String, Map<TopicPartition, List<Replica>>> replicasGetter;
    private final Function<String, Config> configsGetter;
    private String topic;
    private int numberOfPartitions = 1;
    private short numberOfReplicas = 1;
    private final Map<String, String> configs = new HashMap<>();

    CreatorImpl(
        org.apache.kafka.clients.admin.Admin admin,
        Function<String, Map<TopicPartition, List<Replica>>> replicasGetter,
        Function<String, Config> configsGetter) {
      this.admin = admin;
      this.replicasGetter = replicasGetter;
      this.configsGetter = configsGetter;
    }

    @Override
    public TopicCreator topic(String topic) {
      this.topic = Objects.requireNonNull(topic);
      return this;
    }

    @Override
    public TopicCreator numberOfPartitions(int numberOfPartitions) {
      this.numberOfPartitions = numberOfPartitions;
      return this;
    }

    @Override
    public TopicCreator numberOfReplicas(short numberOfReplicas) {
      this.numberOfReplicas = numberOfReplicas;
      return this;
    }

    @Override
    public TopicCreator config(String key, String value) {
      this.configs.put(key, value);
      return this;
    }

    @Override
    public TopicCreator configs(Map<String, String> configs) {
      this.configs.putAll(configs);
      return this;
    }

    @Override
    public void create() {
      if (Utils.packException(() -> admin.listTopics().names().get()).contains(topic)) {
        var partitionReplicas = replicasGetter.apply(topic);
        partitionReplicas.forEach(
            (tp, replicas) -> {
              if (replicas.size() != numberOfReplicas)
                throw new IllegalArgumentException(
                    topic
                        + " is existent but its replicas: "
                        + replicas.size()
                        + " is not equal to expected: "
                        + numberOfReplicas);
            });
        var result =
            Utils.packException(() -> admin.describeTopics(Set.of(topic)).all().get().get(topic));
        if (result.partitions().size() != numberOfPartitions)
          throw new IllegalArgumentException(
              topic
                  + " is existent but its partitions: "
                  + result.partitions().size()
                  + " is not equal to expected: "
                  + numberOfReplicas);

        var actualConfigs = configsGetter.apply(topic);
        this.configs.forEach(
            (key, value) -> {
              if (actualConfigs.value(key).filter(actual -> actual.equals(value)).isEmpty())
                throw new IllegalArgumentException(
                    topic
                        + " is existent but its config: <"
                        + key
                        + ", "
                        + actualConfigs.value(key)
                        + "> is not equal to expected: "
                        + key
                        + ", "
                        + value);
            });

        // ok, the existent topic is totally equal to what we want to create.
        return;
      }

      Utils.packException(
          () ->
              admin
                  .createTopics(
                      List.of(
                          new NewTopic(topic, numberOfPartitions, numberOfReplicas)
                              .configs(configs)))
                  .all()
                  .get());
    }
  }

  private static class MigratorImpl implements ReplicaMigrator {
    private final org.apache.kafka.clients.admin.Admin admin;
    private final Function<Set<String>, Set<TopicPartition>> partitionGetter;
    private final Set<TopicPartition> partitions = new HashSet<>();
    private boolean updateLeader = false;

    MigratorImpl(
        org.apache.kafka.clients.admin.Admin admin,
        Function<Set<String>, Set<TopicPartition>> partitionGetter) {
      this.admin = admin;
      this.partitionGetter = partitionGetter;
    }

    @Override
    public ReplicaMigrator topic(String topic) {
      partitions.addAll(partitionGetter.apply(Set.of(topic)));
      return this;
    }

    @Override
    public ReplicaMigrator partition(String topic, int partition) {
      partitions.add(new TopicPartition(topic, partition));
      return this;
    }

    @Override
    public void moveTo(Map<Integer, String> brokerFolders) {
      Utils.packException(
          () ->
              admin
                  .alterReplicaLogDirs(
                      brokerFolders.entrySet().stream()
                          .collect(
                              Collectors.toMap(
                                  x ->
                                      TopicPartitionReplica.to(
                                          new TopicPartitionReplica(
                                              partitions.iterator().next().topic(),
                                              partitions.iterator().next().partition(),
                                              x.getKey())),
                                  Map.Entry::getValue)))
                  .all()
                  .get());
    }

    @Override
    public void moveTo(List<Integer> brokers) {
      Utils.packException(
          () ->
              admin
                  .alterPartitionReassignments(
                      partitions.stream()
                          .collect(
                              Collectors.toMap(
                                  TopicPartition::to,
                                  ignore -> Optional.of(new NewPartitionReassignment(brokers)))))
                  .all()
                  .get());
    }
  }

  private static class QuotaImpl implements QuotaCreator {
    private final org.apache.kafka.clients.admin.Admin admin;

    QuotaImpl(org.apache.kafka.clients.admin.Admin admin) {
      this.admin = admin;
    }

    @Override
    public Ip ip(String ip) {
      return new Ip() {
        private int connectionRate = Integer.MAX_VALUE;

        @Override
        public Ip connectionRate(int value) {
          this.connectionRate = value;
          return this;
        }

        @Override
        public void create() {
          if (connectionRate == Integer.MAX_VALUE) return;
          Utils.packException(
              () ->
                  admin
                      .alterClientQuotas(
                          List.of(
                              new ClientQuotaAlteration(
                                  new ClientQuotaEntity(Map.of(ClientQuotaEntity.IP, ip)),
                                  List.of(
                                      new ClientQuotaAlteration.Op(
                                          Quota.Limit.IP_CONNECTION_RATE.nameOfKafka(),
                                          (double) connectionRate)))))
                      .all()
                      .get());
        }
      };
    }

    @Override
    public Client clientId(String id) {
      return new Client() {
        private int produceRate = Integer.MAX_VALUE;
        private int consumeRate = Integer.MAX_VALUE;

        @Override
        public Client produceRate(int value) {
          this.produceRate = value;
          return this;
        }

        @Override
        public Client consumeRate(int value) {
          this.consumeRate = value;
          return this;
        }

        @Override
        public void create() {
          var q = new ArrayList<ClientQuotaAlteration.Op>();
          if (produceRate != Integer.MAX_VALUE)
            q.add(
                new ClientQuotaAlteration.Op(
                    Quota.Limit.PRODUCER_BYTE_RATE.nameOfKafka(), (double) produceRate));
          if (consumeRate != Integer.MAX_VALUE)
            q.add(
                new ClientQuotaAlteration.Op(
                    Quota.Limit.CONSUMER_BYTE_RATE.nameOfKafka(), (double) consumeRate));
          if (!q.isEmpty())
            Utils.packException(
                () ->
                    admin
                        .alterClientQuotas(
                            List.of(
                                new ClientQuotaAlteration(
                                    new ClientQuotaEntity(Map.of(ClientQuotaEntity.CLIENT_ID, id)),
                                    q)))
                        .all()
                        .get());
        }
      };
    }
  }
}
