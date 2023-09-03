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
package org.astraea.common.cost;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.broker.HasMaxRate;

public class MigrationCost {

  public final String name;
  public final Map<Integer, Long> brokerCosts;
  public static final String TO_SYNC_BYTES = "record size to sync (bytes)";
  public static final String TO_FETCH_BYTES = "record size to fetch (bytes)";
  public static final String REPLICA_LEADERS_TO_ADDED = "leader number to add";
  public static final String REPLICA_LEADERS_TO_REMOVE = "leader number to remove";
  public static final String CHANGED_REPLICAS = "changed replicas";
  public static final String MIGRATION_ELAPSED_TIME = "migration elapsed time";

  public static List<MigrationCost> migrationCosts(
      ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    var migrateInBytes = recordSizeToSync(before, after);
    var migrateOutBytes = recordSizeToFetch(before, after);
    var migrateReplicaNum = replicaNumChanged(before, after);
    var migrateInLeader = replicaLeaderToAdd(before, after);
    var migrateOutLeader = replicaLeaderToRemove(before, after);
    return List.of(
        new MigrationCost(TO_SYNC_BYTES, migrateInBytes),
        new MigrationCost(TO_FETCH_BYTES, migrateOutBytes),
        new MigrationCost(CHANGED_REPLICAS, migrateReplicaNum),
        new MigrationCost(
            MIGRATION_ELAPSED_TIME, brokerMigrationSecond(before, after, clusterBean)),
        new MigrationCost(REPLICA_LEADERS_TO_ADDED, migrateInLeader),
        new MigrationCost(REPLICA_LEADERS_TO_REMOVE, migrateOutLeader),
        new MigrationCost(CHANGED_REPLICAS, migrateReplicaNum));
  }

  public MigrationCost(String name, Map<Integer, Long> brokerCosts) {
    this.name = name;
    this.brokerCosts = brokerCosts;
  }

  static Map<Integer, Long> recordSizeToFetch(ClusterInfo before, ClusterInfo after) {
    return migratedChanged(before, after, true, (ignore) -> true, Replica::size);
  }

  static Map<Integer, Long> recordSizeToSync(ClusterInfo before, ClusterInfo after) {
    return migratedChanged(before, after, false, (ignore) -> true, Replica::size);
  }

  static Map<Integer, Long> replicaNumChanged(ClusterInfo before, ClusterInfo after) {
    return changedReplicaNumber(before, after);
  }

  static Map<Integer, Long> replicaLeaderToAdd(ClusterInfo before, ClusterInfo after) {
    return migratedChanged(before, after, true, Replica::isLeader, ignore -> 1L);
  }

  static Map<Integer, Long> replicaLeaderToRemove(ClusterInfo before, ClusterInfo after) {
    return migratedChanged(before, after, false, Replica::isLeader, ignore -> 1L);
  }

  /**
   * @param before the ClusterInfo before migrated replicas
   * @param after the ClusterInfo after migrated replicas
   * @param clusterBean cluster metrics
   * @return estimated migrated time required by all brokers (seconds)
   */
  public static Map<Integer, Long> brokerMigrationSecond(
      ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    var brokerInRate =
        before.brokers().stream()
            .collect(
                Collectors.toMap(
                    Broker::id,
                    nodeInfo ->
                        brokerMaxRate(
                            nodeInfo.id(),
                            clusterBean,
                            MigrateTimeCost.MaxReplicationInRateBean.class)));
    var brokerOutRate =
        before.brokers().stream()
            .collect(
                Collectors.toMap(
                    Broker::id,
                    nodeInfo ->
                        brokerMaxRate(
                            nodeInfo.id(),
                            clusterBean,
                            MigrateTimeCost.MaxReplicationOutRateBean.class)));
    var brokerMigrateInSecond =
        MigrationCost.recordSizeToSync(before, after).entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    brokerSize ->
                        brokerSize.getValue() / brokerInRate.get(brokerSize.getKey()).orElse(0.0)));
    var brokerMigrateOutSecond =
        MigrationCost.recordSizeToFetch(before, after).entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    brokerSize ->
                        brokerSize.getValue()
                            / brokerOutRate.get(brokerSize.getKey()).orElse(0.0)));
    return Stream.concat(before.brokers().stream(), after.brokers().stream())
        .map(Broker::id)
        .distinct()
        .collect(
            Collectors.toMap(
                nodeId -> nodeId,
                nodeId ->
                    (long)
                        Math.max(
                            brokerMigrateInSecond.get(nodeId),
                            brokerMigrateOutSecond.get(nodeId))));
  }

  static Optional<Double> brokerMaxRate(
      int identity, ClusterBean clusterBean, Class<? extends HasBeanObject> statisticMetrics) {
    return clusterBean
        .brokerMetrics(identity, statisticMetrics)
        .filter(b -> b instanceof HasMaxRate)
        .map(b -> ((HasMaxRate) b).maxRate())
        .max(Comparator.naturalOrder());
  }

  /**
   * @param before the ClusterInfo before migrated replicas
   * @param after the ClusterInfo after migrated replicas
   * @param migrateOut if data log need fetch from replica leader, set this true
   * @param predicate used to filter replicas
   * @param replicaFunction decide what information you want to calculate for the replica
   * @return the data size to migrated by all brokers
   */
  private static Map<Integer, Long> migratedChanged(
      ClusterInfo before,
      ClusterInfo after,
      boolean migrateOut,
      Predicate<Replica> predicate,
      Function<Replica, Long> replicaFunction) {
    var source = migrateOut ? after : before;
    var dest = migrateOut ? before : after;
    var changePartitions = ClusterInfo.findNonFulfilledAllocation(source, dest);
    var cost =
        changePartitions.stream()
            .flatMap(
                p ->
                    dest.replicas(p).stream()
                        .filter(predicate)
                        .filter(
                            r -> source.replicas(p).stream().noneMatch(x -> checkoutSameTPR(r, x))))
            .map(
                r -> {
                  if (migrateOut) return dest.replicaLeader(r.topicPartition()).orElse(r);
                  return r;
                })
            .collect(
                Collectors.groupingBy(
                    Replica::brokerId,
                    Collectors.mapping(
                        Function.identity(), Collectors.summingLong(replicaFunction::apply))));
    return Stream.concat(dest.brokers().stream(), source.brokers().stream())
        .map(Broker::id)
        .distinct()
        .parallel()
        .collect(Collectors.toMap(Function.identity(), n -> cost.getOrDefault(n, 0L)));
  }

  private static boolean checkoutSameTPR(Replica r1, Replica r2) {
    return r1.topicPartitionReplica().equals(r2.topicPartitionReplica());
  }

  private static Map<Integer, Long> changedReplicaNumber(ClusterInfo before, ClusterInfo after) {
    return Stream.concat(before.brokers().stream(), after.brokers().stream())
        .map(Broker::id)
        .distinct()
        .parallel()
        .collect(
            Collectors.toUnmodifiableMap(
                Function.identity(),
                id -> {
                  var removedLeaders =
                      before
                          .replicaStream(id)
                          .filter(
                              r ->
                                  after
                                      .replicaStream(r.topicPartitionReplica())
                                      .findAny()
                                      .isEmpty())
                          .count();
                  var newLeaders =
                      after
                          .replicaStream(id)
                          .filter(
                              r ->
                                  before
                                      .replicaStream(r.topicPartitionReplica())
                                      .findAny()
                                      .isEmpty())
                          .count();
                  return newLeaders - removedLeaders;
                }));
  }
}
