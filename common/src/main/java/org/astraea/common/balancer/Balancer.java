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

import java.util.Optional;
import org.astraea.common.Configuration;
import org.astraea.common.EnumInfo;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.balancer.algorithms.SingleStepBalancer;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.metrics.ClusterBean;

public interface Balancer {

  /**
   * @return a rebalance plan
   */
  Optional<Plan> offer(AlgorithmConfig config);

  /**
   * @param initialClusterCost The {@link ClusterCost} score of the original {@link ClusterInfo}
   *     when this plan is start generating.
   * @param proposalClusterCost The {@link ClusterCost} score of the proposed new allocation.
   */
  record Plan(
      ClusterBean clusterBean,
      ClusterInfo initialClusterInfo,
      ClusterCost initialClusterCost,
      ClusterInfo proposal,
      ClusterCost proposalClusterCost) {}

  /** The official implementation of {@link Balancer}. */
  enum Official implements EnumInfo {
    SingleStep(SingleStepBalancer.class),
    Greedy(GreedyBalancer.class);

    private final Class<? extends Balancer> balancerClass;

    Official(Class<? extends Balancer> theClass) {
      this.balancerClass = theClass;
    }

    public Class<? extends Balancer> theClass() {
      return balancerClass;
    }

    public Balancer create() {
      return Utils.construct(theClass(), Configuration.EMPTY);
    }

    @Override
    public String alias() {
      return this.name();
    }

    @Override
    public String toString() {
      return alias();
    }

    public static Official ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(Official.class, alias);
    }
  }
}
