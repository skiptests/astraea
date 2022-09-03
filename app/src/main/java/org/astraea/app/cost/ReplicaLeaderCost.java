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
package org.astraea.app.cost;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.Replica;
import org.astraea.app.admin.ReplicaInfo;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.broker.ServerMetrics;
import org.astraea.app.metrics.collector.Fetcher;

/** more replica leaders -> higher cost */
public class ReplicaLeaderCost implements HasBrokerCost, HasClusterCost, HasMoveCost {
  private final Dispersion dispersion = Dispersion.correlationCoefficient();

  @Override
  public BrokerCost brokerCost(
      ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean) {
    var result =
        leaderCount(clusterInfo, clusterBean).entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (double) e.getValue()));
    return () -> result;
  }

  @Override
  public ClusterCost clusterCost(ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
    var brokerScore = brokerCost(clusterInfo, clusterBean).value();
    return () -> dispersion.calculate(brokerScore.values());
  }

  private static Map<Integer, Integer> leaderCount(
      ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean) {
    var leaderCount = leaderCount(clusterBean);
    // if there is no available metrics, we re-count the leaders based on cluster information
    if (leaderCount.values().stream().mapToInt(i -> i).sum() == 0) return leaderCount(clusterInfo);
    return leaderCount;
  }

  static Map<Integer, Integer> leaderCount(ClusterBean clusterBean) {
    return clusterBean.all().entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e ->
                    e.getValue().stream()
                        .filter(x -> x instanceof ServerMetrics.ReplicaManager.Gauge)
                        .map(x -> (ServerMetrics.ReplicaManager.Gauge) x)
                        .sorted(Comparator.comparing(HasBeanObject::createdTimestamp).reversed())
                        .limit(1)
                        .mapToInt(v -> (int) v.value())
                        .sum()));
  }

  static Map<Integer, Integer> leaderCount(ClusterInfo<? extends ReplicaInfo> clusterInfo) {
    return clusterInfo.replicaLeaders().stream()
        .collect(Collectors.groupingBy(r -> r.nodeInfo().id()))
        .entrySet()
        .stream()
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> e.getValue().size()));
  }

  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.of(c -> List.of(ServerMetrics.ReplicaManager.LEADER_COUNT.fetch(c)));
  }

  @Override
  public MoveCost moveCost(
      ClusterInfo<Replica> before, ClusterInfo<Replica> after, ClusterBean clusterBean) {
    var removedReplicas = ClusterInfo.diff(before, after);
    var addedReplicas = ClusterInfo.diff(after, before);
    var migrateInfo = migrateInfo(removedReplicas, addedReplicas);
    var leaderNumChanges = migrateInfo.replicaNumChange;
    var totalLeaderNum = migrateInfo.totalMigrateNum;
    return new MoveCost() {
      @Override
      public String name() {
        return "replica number";
      }

      @Override
      public long totalCost() {
        return totalLeaderNum;
      }

      @Override
      public String unit() {
        return "byte";
      }

      @Override
      public Map<Integer, Long> changes() {
        return leaderNumChanges;
      }
    };
  }

  static class MigrateInfo {
    long totalMigrateNum;
    Map<Integer, Long> replicaNumChange;

    MigrateInfo(long totalMigrateNum, Map<Integer, Long> replicaNumChange) {
      this.totalMigrateNum = totalMigrateNum;
      this.replicaNumChange = replicaNumChange;
    }
  }

  static MigrateInfo migrateInfo(
      Collection<Replica> removedReplicas, Collection<Replica> addedReplicas) {
    var changes = new HashMap<Integer, Long>();
    AtomicLong totalMigrateNum = new AtomicLong(0L);
    removedReplicas.forEach(
        replica -> {
          if (replica.isLeader())
            changes.compute(
                replica.nodeInfo().id(), (ignore, size) -> (size == null) ? -1 : size - 1);
        });
    addedReplicas.forEach(
        replica -> {
          if (replica.isLeader())
            changes.compute(
                replica.nodeInfo().id(),
                (ignore, size) -> {
                  totalMigrateNum.set(totalMigrateNum.get() + 1);
                  return (size == null) ? 1 : size + 1;
                });
        });
    return new MigrateInfo(totalMigrateNum.get(), changes);
  }
}
