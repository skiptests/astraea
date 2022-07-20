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
package org.astraea.app.balancer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.balancer.generator.ShufflePlanGenerator;
import org.astraea.app.balancer.log.LayeredClusterLogAllocation;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.app.balancer.metrics.IdentifiedFetcher;
import org.astraea.app.balancer.metrics.MetricSource;
import org.astraea.app.balancer.utils.DummyCostFunction;
import org.astraea.app.balancer.utils.DummyExecutor;
import org.astraea.app.balancer.utils.DummyGenerator;
import org.astraea.app.balancer.utils.DummyMetricSource;
import org.astraea.app.cost.ClusterInfoProvider;
import org.astraea.app.partitioner.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BalancerUtilsTest {

  private static final Configuration emptyConfig = Configuration.of(Map.of());

  static class DummyConfigMetricSource extends DummyMetricSource {
    public DummyConfigMetricSource(Configuration c, Collection<IdentifiedFetcher> something) {
      super(c, something);
    }
  }

  static class DummyConfigCostFunction extends DummyCostFunction {
    public DummyConfigCostFunction(Configuration configuration) {}
  }

  static class DummyConfigGenerator extends DummyGenerator {
    public DummyConfigGenerator(Configuration configuration) {}
  }

  static class DummyConfigExecutor extends DummyExecutor {
    public DummyConfigExecutor(Configuration configuration) {}
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 4})
  void testMockClusterInfo(int gotoBroker) {
    // arrange
    var nodeCount = 5;
    var topicCount = 3;
    var partitionCount = 5;
    var replicaCount = 1;
    var logCount = topicCount * partitionCount * replicaCount;
    var dir = "/path/to/somewhere";
    var topicNames =
        IntStream.range(0, topicCount)
            .mapToObj(i -> "Topic_" + i)
            .collect(Collectors.toUnmodifiableSet());
    var clusterInfo =
        ClusterInfoProvider.fakeClusterInfo(
            nodeCount, topicCount, partitionCount, replicaCount, (ignore) -> topicNames);
    var mockedAllocationMap =
        topicNames.stream()
            .flatMap(
                topicName ->
                    IntStream.range(0, partitionCount)
                        .mapToObj(p -> new TopicPartition(topicName, p)))
            .collect(
                Collectors.toUnmodifiableMap(
                    i -> i, i -> List.of(LogPlacement.of(gotoBroker, dir))));
    var mockedAllocation = LayeredClusterLogAllocation.of(mockedAllocationMap);

    // act, declare every log should locate at specific broker -> gotoBroker
    var mockedClusterInfo = BalancerUtils.mockClusterInfoAllocation(clusterInfo, mockedAllocation);

    // assert, expected every log in the mocked ClusterInfo locate at that broker
    Assertions.assertEquals(
        Collections.nCopies(logCount, gotoBroker),
        topicNames.stream()
            .flatMap(name -> mockedClusterInfo.replicas(name).stream())
            .map(replica -> replica.nodeInfo().id())
            .collect(Collectors.toUnmodifiableList()));

    // assert, expected every log in the mocked ClusterInfo locate at that dir
    Assertions.assertEquals(
        Collections.nCopies(logCount, dir),
        topicNames.stream()
            .flatMap(name -> mockedClusterInfo.replicas(name).stream())
            .map(replica -> replica.dataFolder().orElseThrow())
            .collect(Collectors.toUnmodifiableList()));
  }

  @Test
  void testNewInstance() {
    // Object
    Assertions.assertEquals(
        Object.class, BalancerUtils.newInstance(Object.class).orElseThrow().getClass());
    // AtomicBoolean
    Assertions.assertEquals(
        AtomicBoolean.class,
        BalancerUtils.newInstance(AtomicBoolean.class).orElseThrow().getClass());
    // ArrayList
    Assertions.assertEquals(
        ArrayList.class, BalancerUtils.newInstance(ArrayList.class).orElseThrow().getClass());

    // ShufflePlanGenerator
    Assertions.assertEquals(
        ShufflePlanGenerator.class,
        BalancerUtils.newInstance(ShufflePlanGenerator.class, Configuration.of(Map.of()))
            .orElseThrow()
            .getClass());
    // AtomicInteger
    Assertions.assertEquals(
        5566, BalancerUtils.newInstance(AtomicInteger.class, 5566).orElseThrow().get());
    Assertions.assertEquals(
        new TopicPartition("topic", 32),
        BalancerUtils.newInstance(TopicPartition.class, "topic", 32).orElseThrow());
  }

  @Test
  void testConstructMetricSource() {
    // two arg constructor
    try (MetricSource metricSource =
        BalancerUtils.constructMetricSource(
            DummyConfigMetricSource.class, emptyConfig, List.of())) {
      Assertions.assertEquals(DummyConfigMetricSource.class, metricSource.getClass());
    }
  }

  @Test
  void testConstructCostFunction() {
    // test no arg constructor
    Assertions.assertEquals(
        DummyCostFunction.class,
        BalancerUtils.constructCostFunction(DummyCostFunction.class, emptyConfig).getClass());

    // test config arg constructor
    Assertions.assertEquals(
        DummyConfigCostFunction.class,
        BalancerUtils.constructCostFunction(DummyConfigCostFunction.class, emptyConfig).getClass());
  }

  @Test
  void testConstructGenerator() {
    // test no arg constructor
    Assertions.assertEquals(
        DummyGenerator.class,
        BalancerUtils.constructGenerator(DummyGenerator.class, emptyConfig).getClass());

    // test config arg constructor
    Assertions.assertEquals(
        DummyConfigGenerator.class,
        BalancerUtils.constructGenerator(DummyConfigGenerator.class, emptyConfig).getClass());
  }

  @Test
  void testConstructExecutor() {
    // test no arg constructor
    Assertions.assertEquals(
        DummyExecutor.class,
        BalancerUtils.constructExecutor(DummyExecutor.class, emptyConfig).getClass());

    // test config arg constructor
    Assertions.assertEquals(
        DummyConfigExecutor.class,
        BalancerUtils.constructExecutor(DummyConfigExecutor.class, emptyConfig).getClass());
  }
}
