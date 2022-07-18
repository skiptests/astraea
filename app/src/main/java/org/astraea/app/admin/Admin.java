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

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface Admin extends Closeable {

  static Builder builder() {
    return new Builder();
  }

  static Admin of(String bootstrapServers) {
    return builder().bootstrapServers(bootstrapServers).build();
  }

  static Admin of(Map<String, String> configs) {
    return builder().configs(configs).build();
  }

  /** @return names of all topics */
  Set<String> topicNames();

  /** @return the topic name and its configurations. */
  default Map<String, Config> topics() {
    return topics(topicNames());
  }

  /** @return the topic name and its configurations. */
  Map<String, Config> topics(Set<String> topicNames);

  /** @return all partitions */
  default Set<TopicPartition> partitions() {
    return partitions(topicNames());
  }

  /**
   * @param topics target
   * @return the partitions belong to input topics
   */
  Set<TopicPartition> partitions(Set<String> topics);

  /** @return a topic creator to set all topic configs and then run the procedure. */
  TopicCreator creator();

  /** @return offsets of all partitions */
  default Map<TopicPartition, Offset> offsets() {
    return offsets(topicNames());
  }

  /**
   * @param topics topic names
   * @return the earliest offset and latest offset for specific topics
   */
  Map<TopicPartition, Offset> offsets(Set<String> topics);

  /** @return all consumer groups */
  default Map<String, ConsumerGroup> consumerGroups() {
    return consumerGroups(consumerGroupIds());
  }

  /** @return all consumer group ids */
  Set<String> consumerGroupIds();

  /**
   * @param consumerGroupNames consumer group names.
   * @return the member info of each consumer group
   */
  Map<String, ConsumerGroup> consumerGroups(Set<String> consumerGroupNames);

  /** @return replica info of all partitions */
  default Map<TopicPartition, List<Replica>> replicas() {
    return replicas(topicNames());
  }

  /**
   * @param topics topic names
   * @return the replicas of partition
   */
  Map<TopicPartition, List<Replica>> replicas(Set<String> topics);

  /** @return all broker id and their configuration */
  default Map<Integer, Config> brokers() {
    return brokers(brokerIds());
  }

  /**
   * @param brokerIds to search
   * @return broker information
   */
  Map<Integer, Config> brokers(Set<Integer> brokerIds);

  /** @return all alive brokers' ids */
  default Set<Integer> brokerIds() {
    return nodes().stream().map(NodeInfo::id).collect(Collectors.toUnmodifiableSet());
  }

  /** @return all alive node information in the cluster */
  Set<NodeInfo> nodes();

  /**
   * list all partitions belongs to input brokers
   *
   * @param brokerId to search
   * @return all partition belongs to brokers
   */
  default Set<TopicPartition> partitions(int brokerId) {
    return partitions(topicNames(), Set.of(brokerId)).getOrDefault(brokerId, Set.of());
  }

  /**
   * @param topics topic names
   * @param brokerIds brokers ID
   * @return the partitions of brokers
   */
  Map<Integer, Set<TopicPartition>> partitions(Set<String> topics, Set<Integer> brokerIds);

  /** @return data folders of all broker nodes */
  default Map<Integer, Set<String>> brokerFolders() {
    return brokerFolders(brokerIds());
  }

  /**
   * @param brokers a Set containing broker's ID
   * @return all log directory
   */
  Map<Integer, Set<String>> brokerFolders(Set<Integer> brokers);

  /** @return a partition migrator used to move partitions to another broker or folder. */
  ReplicaMigrator migrator();

  /**
   * Perform preferred leader election for the specified topic/partitions. Let the first replica(the
   * preferred leader) in the partition replica list becomes the leader of its corresponding
   * topic/partition. Noted that the first replica(the preferred leader) must be in-sync state.
   * Otherwise, an exception might be raised.
   *
   * @param topicPartition to perform preferred leader election
   */
  void preferredLeaderElection(TopicPartition topicPartition);

  /** @return producer states of all topic partitions */
  default Map<TopicPartition, Collection<ProducerState>> producerStates() {
    return producerStates(partitions());
  }

  /**
   * @param partitions to search
   * @return producer states of input topic partitions
   */
  Map<TopicPartition, Collection<ProducerState>> producerStates(Set<TopicPartition> partitions);

  /** @return a progress to set quota */
  QuotaCreator quotaCreator();

  /**
   * @param target to search
   * @return quotas
   */
  Collection<Quota> quotas(Quota.Target target);

  /**
   * @param target to search
   * @param value assoicated to target
   * @return quotas
   */
  Collection<Quota> quotas(Quota.Target target, String value);

  /** @return all quotas */
  Collection<Quota> quotas();

  /** @return a snapshot object of cluster state at the moment */
  default ClusterInfo clusterInfo() {
    return clusterInfo(topicNames());
  }

  /**
   * @param topics query only this subset of topics
   * @return a snapshot object of cluster state at the moment
   */
  ClusterInfo clusterInfo(Set<String> topics);

  /** @return all transaction ids */
  Set<String> transactionIds();

  /** @return all transaction states */
  default Map<String, Transaction> transactions() {
    return transactions(transactionIds());
  }
  /**
   * return transaction states associated to input ids
   *
   * @param transactionIds to query state
   * @return transaction states
   */
  Map<String, Transaction> transactions(Set<String> transactionIds);

  /**
   * remove an empty group. It causes error if the group has memebrs.
   *
   * @param groupId to remove
   */
  void removeGroup(String groupId);

  /** @param groupId to remove all (dynamic and static) members */
  void removeAllMembers(String groupId);

  /**
   * @param groupId to remove static members
   * @param members group instance id (static member)
   */
  void removeStaticMembers(String groupId, Set<String> members);

  /**
   * Get the reassignments of all topics.
   *
   * @return reassignment
   */
  default Map<TopicPartition, Reassignment> reassignments() {
    return reassignments(topicNames());
  }

  /**
   * Get the reassignments of topics. It returns nothing if the partitions are not migrating.
   *
   * @param topics to search
   * @return reassignment
   */
  Map<TopicPartition, Reassignment> reassignments(Set<String> topics);

  /** Return a utility instance for altering replication throttle. */
  ReplicationThrottler replicationThrottler();

  @Override
  void close();
}
