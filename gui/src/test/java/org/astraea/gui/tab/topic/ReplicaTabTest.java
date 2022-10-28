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
package org.astraea.gui.tab.topic;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.gui.Context;
import org.astraea.gui.pane.Input;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReplicaTabTest extends RequireBrokerCluster {

  @Test
  void testTableAction() throws ExecutionException, InterruptedException {
    var topicName = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topicName)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 1)
          .run()
          .toCompletableFuture()
          .get();
      Utils.sleep(Duration.ofSeconds(2));

      var action = ReplicaTab.tableViewAction(new Context(admin));
      var log = new AtomicReference<String>();
      var f = action.apply(List.of(), Input.of(List.of(), Map.of()), log::set);
      Assertions.assertTrue(f.toCompletableFuture().isDone());
      Assertions.assertEquals("nothing to alert", log.get());

      var f2 =
          action.apply(
              List.of(Map.of(ReplicaTab.TOPIC_NAME_KEY, topicName, ReplicaTab.PARTITION_KEY, 0)),
              Input.of(List.of(), Map.of()),
              log::set);
      Assertions.assertTrue(f2.toCompletableFuture().isDone());
      Assertions.assertEquals("please define " + ReplicaTab.MOVE_BROKER_KEY, log.get());

      var f3 =
          action.apply(
              List.of(Map.of(ReplicaTab.TOPIC_NAME_KEY, topicName, ReplicaTab.PARTITION_KEY, 0)),
              Input.of(
                  List.of(),
                  Map.of(
                      ReplicaTab.MOVE_BROKER_KEY,
                      Optional.of(
                          brokerIds().stream()
                              .map(String::valueOf)
                              .collect(Collectors.joining(","))))),
              log::set);
      f3.toCompletableFuture().get();
      Assertions.assertEquals("succeed to alter partitions: [" + topicName + "-0]", log.get());
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(
          3, admin.replicas(Set.of(topicName)).toCompletableFuture().get().size());
    }
  }

  @Test
  void testResult() {
    var topic = Utils.randomString();
    var partition = 0;
    var leaderSize = 100;
    var replicas =
        List.of(
            Replica.builder()
                .leader(true)
                .topic(topic)
                .partition(partition)
                .nodeInfo(NodeInfo.of(0, "aa", 0))
                .size(leaderSize)
                .path("/tmp/aaa")
                .build(),
            Replica.builder()
                .leader(false)
                .topic(topic)
                .partition(partition)
                .nodeInfo(NodeInfo.of(1, "aa", 0))
                .size(20)
                .build(),
            Replica.builder()
                .leader(false)
                .topic(topic)
                .partition(partition)
                .nodeInfo(NodeInfo.of(2, "aa", 0))
                .size(30)
                .build());
    var results = ReplicaTab.allResult(replicas);
    Assertions.assertEquals(3, results.size());
    Assertions.assertEquals(
        1,
        results.stream()
            .filter(
                m ->
                    !m.containsKey(ReplicaTab.LEADER_SIZE_KEY)
                        && !m.containsKey(ReplicaTab.PROGRESS_KEY))
            .count());
    Assertions.assertEquals(
        2,
        results.stream()
            .filter(
                m ->
                    m.containsKey(ReplicaTab.LEADER_SIZE_KEY)
                        && m.containsKey(ReplicaTab.PROGRESS_KEY))
            .count());
    Assertions.assertEquals(
        1, results.stream().filter(m -> m.containsKey(ReplicaTab.PATH_KEY)).count());
    Assertions.assertEquals(
        Set.of("30.00%", "20.00%"),
        results.stream()
            .filter(
                m ->
                    m.containsKey(ReplicaTab.LEADER_SIZE_KEY)
                        && m.containsKey(ReplicaTab.PROGRESS_KEY))
            .map(m -> m.get(ReplicaTab.PROGRESS_KEY))
            .collect(Collectors.toSet()));
  }
}
