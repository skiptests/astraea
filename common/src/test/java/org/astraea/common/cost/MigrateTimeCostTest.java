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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.ClusterBean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MigrateTimeCostTest {

  private static final BeanObject inBean0 =
      new BeanObject("domain", Map.of(), Map.of(MigrateTimeCost.REPLICATION_RATE, 1000.0));
  private static final BeanObject outBean0 =
      new BeanObject("domain", Map.of(), Map.of(MigrateTimeCost.REPLICATION_RATE, 1500.0));
  private static final BeanObject inBean1 =
      new BeanObject("domain", Map.of(), Map.of(MigrateTimeCost.REPLICATION_RATE, 2000.0));
  private static final BeanObject outBean1 =
      new BeanObject("domain", Map.of(), Map.of(MigrateTimeCost.REPLICATION_RATE, 2500.0));
  private static final BeanObject inBean2 =
      new BeanObject("domain", Map.of(), Map.of(MigrateTimeCost.REPLICATION_RATE, 3000.0));
  private static final BeanObject outBean2 =
      new BeanObject("domain", Map.of(), Map.of(MigrateTimeCost.REPLICATION_RATE, 3500.0));

  @Test
  void testMigratedCost() {
    // before(partition-broker): p10-0, p11-1, p12-2, p12-0
    // after(partition-broker):  p10-1, p11-2, p12-0, p12-1
    // b0: migrate in:  ; migrate out: p10
    // b1: migrate in: p10,p12 ; migrate out: p11
    // b2: migrate in: p11;  migrate out: p12
    var before = of(before(), brokers());
    var after = of(after(), brokers());
    var migrationCost = MigrationCost.brokerMigrationSecond(before, after, clusterBean());
    Assertions.assertEquals(Math.max(0, 10000000 / 1500), migrationCost.get(0));
    Assertions.assertEquals(
        Math.max((10000000 + 30000000) / 2000, 20000000 / 2500), migrationCost.get(1));
    Assertions.assertEquals(Math.max(20000000 / 3000, 30000000 / 3500), migrationCost.get(2));
  }

  private List<Broker> brokers() {
    return before().stream()
        .map(Replica::brokerId)
        .distinct()
        .map(nodeId -> Broker.of(nodeId, "", -1))
        .collect(Collectors.toList());
  }

  @Test
  void testMostCost() {
    var before = of(before(), brokers());
    var after = of(after(), brokers());
    var timeLimit = new Configuration(Map.of(MigrateTimeCost.MAX_MIGRATE_TIME_KEY, "20000"));
    var overFlowTimeLimit =
        new Configuration(Map.of(MigrateTimeCost.MAX_MIGRATE_TIME_KEY, "19999"));
    var cf = new MigrateTimeCost(timeLimit);
    var overFlowCf = new MigrateTimeCost(overFlowTimeLimit);
    var moveCost = cf.moveCost(before, after, clusterBean());
    var overflowCost = overFlowCf.moveCost(before, after, clusterBean());
    Assertions.assertFalse(moveCost.overflow());
    Assertions.assertTrue(overflowCost.overflow());
  }

  public static ClusterInfo of(List<Replica> replicas, List<Broker> nodeInfos) {
    return ClusterInfo.of("fake", nodeInfos, Map.of(), replicas);
  }

  private List<Replica> after() {
    return List.of(
        Replica.builder()
            .topic("t")
            .partition(10)
            .isLeader(true)
            .size(10000000)
            .brokerId(1)
            .build(),
        Replica.builder()
            .topic("t")
            .partition(11)
            .isLeader(true)
            .size(20000000)
            .brokerId(2)
            .build(),
        Replica.builder()
            .topic("t")
            .partition(12)
            .isLeader(true)
            .size(30000000)
            .brokerId(0)
            .build(),
        Replica.builder()
            .topic("t")
            .partition(12)
            .isLeader(false)
            .size(30000000)
            .brokerId(1)
            .build());
  }

  private List<Replica> before() {
    return List.of(
        Replica.builder()
            .topic("t")
            .partition(10)
            .isLeader(true)
            .size(10000000)
            .brokerId(0)
            .build(),
        Replica.builder()
            .topic("t")
            .partition(11)
            .isLeader(true)
            .size(20000000)
            .brokerId(1)
            .build(),
        Replica.builder()
            .topic("t")
            .partition(12)
            .isLeader(true)
            .size(30000000)
            .brokerId(2)
            .build(),
        Replica.builder()
            .topic("t")
            .partition(12)
            .isLeader(false)
            .size(30000000)
            .brokerId(0)
            .build());
  }

  private static ClusterBean clusterBean() {
    return ClusterBean.of(
        Map.of(
            0,
            List.of(
                new MigrateTimeCost.MaxReplicationInRateBean(inBean0),
                new MigrateTimeCost.MaxReplicationOutRateBean(outBean0)),
            1,
            List.of(
                new MigrateTimeCost.MaxReplicationInRateBean(inBean1),
                new MigrateTimeCost.MaxReplicationOutRateBean(outBean1)),
            2,
            List.of(
                new MigrateTimeCost.MaxReplicationInRateBean(inBean2),
                new MigrateTimeCost.MaxReplicationOutRateBean(outBean2))));
  }
}
