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

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.KafkaMetrics;
import org.astraea.app.metrics.broker.HasValue;
import org.astraea.app.metrics.collector.Fetcher;

/** more replica leaders -> higher cost */
public class ReplicaLeaderCost implements HasBrokerCost {

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var result =
        leaderCount(clusterInfo, clusterBean).entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (double) e.getValue()));
    return () -> result;
  }

  Map<Integer, Integer> leaderCount(ClusterInfo ignored, ClusterBean clusterBean) {
    return clusterBean.all().entrySet().stream()
        .flatMap(
            e ->
                e.getValue().stream()
                    .filter(x -> x instanceof HasValue)
                    .filter(x -> "LeaderCount".equals(x.beanObject().properties().get("name")))
                    .filter(x -> "ReplicaManager".equals(x.beanObject().properties().get("type")))
                    .sorted(Comparator.comparing(HasBeanObject::createdTimestamp).reversed())
                    .map(x -> (HasValue) x)
                    .limit(1)
                    .map(e2 -> Map.entry(e.getKey(), (int) e2.value())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.of(KafkaMetrics.ReplicaManager.LeaderCount::fetch);
  }

  public static class NoMetrics extends ReplicaLeaderCost {

    @Override
    Map<Integer, Integer> leaderCount(ClusterInfo clusterInfo, ClusterBean ignored) {
      return clusterInfo.topics().stream()
          .flatMap(t -> clusterInfo.availableReplicaLeaders(t).stream())
          .collect(Collectors.groupingBy(r -> r.nodeInfo().id()))
          .entrySet()
          .stream()
          .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> e.getValue().size()));
    }

    @Override
    public Optional<Fetcher> fetcher() {
      return Optional.empty();
    }
  }
}
