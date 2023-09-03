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
package org.astraea.common.balancer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;

/** A data class represents the skeleton for a balancer problem. */
public class BalancerProblemFormat {

  public String balancer = GreedyBalancer.class.getName();
  public Map<String, String> balancerConfig = Map.of();
  public Map<String, String> costConfig = Map.of();
  public Duration timeout = Duration.ofSeconds(3);
  public List<CostWeight> clusterCosts = List.of();
  public Set<String> moveCosts =
      Set.of(
          "org.astraea.common.cost.ReplicaLeaderCost",
          "org.astraea.common.cost.RecordSizeCost",
          "org.astraea.common.cost.ReplicaNumberCost",
          "org.astraea.common.cost.ReplicaLeaderSizeCost",
          "org.astraea.common.cost.PartitionMigrateTimeCost",
          "org.astraea.common.cost.BrokerDiskSpaceCost");

  public AlgorithmConfig parse() {
    return AlgorithmConfig.builder()
        .timeout(timeout)
        .configs(balancerConfig)
        .clusterCost(clusterCost())
        .moveCost(moveCost())
        .build();
  }

  private HasClusterCost clusterCost() {
    if (clusterCosts.isEmpty()) throw new IllegalArgumentException("clusterCosts is not specified");
    var config = new Configuration(costConfig);
    return HasClusterCost.of(
        Utils.costFunctions(
            clusterCosts.stream()
                .collect(Collectors.toMap(e -> e.cost, e -> String.valueOf(e.weight))),
            HasClusterCost.class,
            config));
  }

  private HasMoveCost moveCost() {
    var config = new Configuration(costConfig);
    var cf = Utils.costFunctions(moveCosts, HasMoveCost.class, config);
    return HasMoveCost.of(cf);
  }

  public static class CostWeight {
    public String cost;
    public double weight = 1.0;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CostWeight that = (CostWeight) o;
      return Double.compare(that.weight, weight) == 0 && Objects.equals(cost, that.cost);
    }

    @Override
    public int hashCode() {
      return Objects.hash(cost, weight);
    }
  }
}
