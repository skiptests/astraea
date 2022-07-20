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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.balancer.generator.RebalancePlanGenerator;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.log.LayeredClusterLogAllocation;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.app.balancer.metrics.IdentifiedFetcher;
import org.astraea.app.balancer.utils.DummyExecutor;
import org.astraea.app.balancer.utils.DummyMetricSource;
import org.astraea.app.common.Utils;
import org.astraea.app.cost.BrokerCost;
import org.astraea.app.cost.HasBrokerCost;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.metrics.jmx.BeanObject;
import org.astraea.app.partitioner.Configuration;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class BalancerTest extends RequireBrokerCluster {

  public static HasBeanObject randomBean() {
    var value = ThreadLocalRandom.current().nextInt();
    BeanObject bean = new BeanObject(Integer.toString(value), Map.of(), Map.of());
    return () -> bean;
  }

  static class RandomCostFunction implements HasBrokerCost {

    public RandomCostFunction(Configuration configuration) {}

    @Override
    public Optional<Fetcher> fetcher() {
      return Optional.of(ignore -> List.of(randomBean()));
    }

    @Override
    public BrokerCost brokerCost(ClusterInfo clusterInfo) {
      var randomScore =
          clusterInfo.nodes().stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      NodeInfo::id, id -> ThreadLocalRandom.current().nextDouble(0, 1)));

      return () -> randomScore;
    }
  }

  static class RandomMetricSource extends DummyMetricSource {
    public RandomMetricSource(
        Configuration configuration, Collection<IdentifiedFetcher> identifiedFetchers) {
      super(configuration, identifiedFetchers);
    }

    @Override
    public Collection<HasBeanObject> metrics(IdentifiedFetcher fetcher, int brokerId) {
      return List.of(randomBean(), randomBean(), randomBean());
    }
  }

  @FunctionalInterface
  interface BalancerScenarioConsumer {
    void run(BalancerConfigs config, Balancer balancer);
  }

  void withPredefinedScenario(
      Set<String> allowedTopics,
      Map<String, String> extraConfig,
      BalancerScenarioConsumer consumer) {
    var predefinedConfig =
        new HashMap<>(
            Map.of(
                BalancerConfigs.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers(),
                BalancerConfigs.JMX_SERVERS_CONFIG,
                String.join(
                    ",",
                    String.format("%d@%s", 0, jmxServiceURL().toString()),
                    String.format("%d@%s", 1, jmxServiceURL().toString()),
                    String.format("%d@%s", 2, jmxServiceURL().toString())),
                BalancerConfigs.BALANCER_COST_FUNCTIONS,
                RandomCostFunction.class.getName(),
                BalancerConfigs.BALANCER_ALLOWED_TOPICS,
                String.join(",", allowedTopics),
                BalancerConfigs.METRICS_SCRAPING_INTERVAL_MS_CONFIG,
                "10",
                BalancerConfigs.METRICS_WARM_UP_COUNT_CONFIG,
                "5",
                BalancerConfigs.BALANCER_PLAN_SEARCHING_ITERATION,
                "100",
                BalancerConfigs.BALANCER_RUN_COUNT,
                "1"));
    predefinedConfig.putAll(extraConfig);
    var config = Configuration.of(predefinedConfig);
    var balancerConfig = new BalancerConfigs(config);
    try (Balancer balancer = Mockito.spy(new Balancer(config))) {
      // permit plan execution no matter what.
      Mockito.when(
              balancer.isPlanExecutionWorth(
                  Mockito.any(), Mockito.any(), Mockito.anyDouble(), Mockito.anyDouble()))
          .thenReturn(true);
      // run test content
      consumer.run(balancerConfig, balancer);
    }
  }

  @Test
  void testRun() {
    var topic = "BalancerTest_testRun_" + Utils.randomString();
    try (Admin admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(10).numberOfReplicas((short) 3).create();
      withPredefinedScenario(
          Set.of(topic),
          Map.of(),
          (ignore, balancer) -> Assertions.assertDoesNotThrow(balancer::run));
    }
  }

  static class FixedPlanGenerator implements RebalancePlanGenerator {
    public FixedPlanGenerator() {}

    static Map<TopicPartition, List<LogPlacement>> thePlan;

    @Override
    public Stream<RebalancePlanProposal> generate(
        ClusterInfo clusterInfo, ClusterLogAllocation baseAllocation) {
      return Stream.generate(
          () ->
              RebalancePlanProposal.builder()
                  .withRebalancePlan(LayeredClusterLogAllocation.of(thePlan))
                  .addInfo("User defined custom plan")
                  .build());
    }
  }

  @Test
  void testPlanHasBeenExecuted() {
    // arrange
    var topic = "BalancerTest_testRun_" + Utils.randomString();
    try (Admin admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(5).numberOfReplicas((short) 3).create();
      var expectedPlacement =
          List.of(
              LogPlacement.of(0, logFolders().get(0).stream().findAny().orElseThrow()),
              LogPlacement.of(1, logFolders().get(1).stream().findAny().orElseThrow()),
              LogPlacement.of(2, logFolders().get(2).stream().findAny().orElseThrow()));
      var expectedAllocation =
          Map.of(
              new TopicPartition(topic, 0), expectedPlacement,
              new TopicPartition(topic, 1), expectedPlacement,
              new TopicPartition(topic, 2), expectedPlacement,
              new TopicPartition(topic, 3), expectedPlacement,
              new TopicPartition(topic, 4), expectedPlacement);
      var currentAllocation =
          (Supplier<Map<TopicPartition, List<LogPlacement>>>)
              () -> {
                var allocation = LayeredClusterLogAllocation.of(admin.clusterInfo(Set.of(topic)));
                return allocation
                    .topicPartitionStream()
                    .collect(Collectors.toUnmodifiableMap(tp -> tp, allocation::logPlacements));
              };
      FixedPlanGenerator.thePlan = expectedAllocation;
      var config =
          Map.of(
              BalancerConfigs.BALANCER_REBALANCE_PLAN_GENERATOR,
              FixedPlanGenerator.class.getName());
      withPredefinedScenario(
          Set.of(topic),
          config,
          (ignore, balancer) -> {
            // act
            Runnable act = balancer::run;

            // assert
            Assertions.assertNotEquals(expectedAllocation, currentAllocation.get());
            act.run();
            Assertions.assertEquals(expectedAllocation, currentAllocation.get());
          });
    }
  }

  @Test
  void testIgnoredTopics() {
    // arrange topic
    var ignoredTopic = "BalancerTest_Ignored_Topic_" + Utils.randomString();
    try (Admin admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(ignoredTopic)
          .numberOfPartitions(3)
          .numberOfReplicas((short) 3)
          .create();
    }
    // arrange mock & spy object around Balancer
    var spiedAdmin = Mockito.spy(Admin.of(bootstrapServers()));
    try (var staticMock = Mockito.mockStatic(Admin.class)) {
      staticMock.when(() -> Admin.of((String) Mockito.any())).thenReturn(spiedAdmin);

      // act
      Runnable run =
          () -> {
            var customConfig =
                Map.of(
                    BalancerConfigs.BALANCER_IGNORED_TOPICS_CONFIG,
                    ignoredTopic,
                    BalancerConfigs.BALANCER_REBALANCE_PLAN_EXECUTOR,
                    DummyExecutor.class.getName());
            withPredefinedScenario(
                Set.of(),
                customConfig,
                (config, balancer) -> {
                  Assertions.assertEquals(Set.of(ignoredTopic), config.ignoredTopics());
                  balancer.run();
                });
          };

      // assert ignored topic is not queried
      Mockito.when(spiedAdmin.replicas(Set.of()))
          .then(
              invocation -> {
                @SuppressWarnings("unchecked")
                var arg0 = (Set<String>) invocation.getArgument(0);
                Assertions.assertFalse(arg0.contains(ignoredTopic));
                return invocation.callRealMethod();
              });
      run.run();
    }
  }

  @Test
  void testAllowedTopics() {
    // arrange topic
    var allowedTopic0 = "BalancerTest_Allowed_Topic_" + Utils.randomString();
    var allowedTopic1 = "BalancerTest_Allowed_Topic_" + Utils.randomString();
    var allowedTopic2 = "BalancerTest_Allowed_Topic_" + Utils.randomString();
    var ignoredTopic = "BalancerTest_Ignored_Topic_" + Utils.randomString();
    try (Admin admin = Admin.of(bootstrapServers())) {
      Stream.of(allowedTopic0, allowedTopic1, allowedTopic2, ignoredTopic)
          .forEach(
              topic ->
                  admin
                      .creator()
                      .topic(topic)
                      .numberOfPartitions(3)
                      .numberOfReplicas((short) 3)
                      .create());
    }
    // arrange mock & spy object around Balancer
    var spiedAdmin = Mockito.spy(Admin.of(bootstrapServers()));
    try (var staticMock = Mockito.mockStatic(Admin.class)) {
      staticMock.when(() -> Admin.of((String) Mockito.any())).thenReturn(spiedAdmin);

      // act
      Runnable run =
          () -> {
            var customConfig =
                Map.of(
                    BalancerConfigs.BALANCER_ALLOWED_TOPICS,
                    String.join(",", allowedTopic0, allowedTopic1, allowedTopic2, ignoredTopic),
                    BalancerConfigs.BALANCER_IGNORED_TOPICS_CONFIG,
                    ignoredTopic,
                    BalancerConfigs.BALANCER_REBALANCE_PLAN_EXECUTOR,
                    DummyExecutor.class.getName());
            withPredefinedScenario(
                Set.of(),
                customConfig,
                (config, balancer) -> {
                  Assertions.assertEquals(
                      Set.of(allowedTopic0, allowedTopic1, allowedTopic2, ignoredTopic),
                      config.allowedTopics());
                  Assertions.assertEquals(Set.of(ignoredTopic), config.ignoredTopics());
                  balancer.run();
                });
          };

      // assert ignored topic is not queried & allowed topics are queried
      Mockito.when(spiedAdmin.replicas(Set.of()))
          .then(
              invocation -> {
                @SuppressWarnings("unchecked")
                var arg0 = (Set<String>) invocation.getArgument(0);
                Assertions.assertFalse(arg0.contains(ignoredTopic));
                Assertions.assertEquals(Set.of(allowedTopic0, allowedTopic1, allowedTopic2), arg0);
                return invocation.callRealMethod();
              });
      run.run();
    }
  }

  static class TestCostFunction0 implements HasBrokerCost {

    public TestCostFunction0(Configuration configuration) {}

    static AtomicReference<Map<Integer, Collection<HasBeanObject>>> metrics =
        new AtomicReference<>();

    static HasBeanObject bean0 = () -> new BeanObject("Bean0", Map.of(), Map.of());

    @Override
    public Optional<Fetcher> fetcher() {
      return Optional.of((ignore) -> List.of(bean0));
    }

    @Override
    public BrokerCost brokerCost(ClusterInfo clusterInfo) {
      metrics.set(clusterInfo.clusterBean().all());
      return () ->
          clusterInfo.nodes().stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      NodeInfo::id, x -> ThreadLocalRandom.current().nextDouble(0, 1)));
    }
  }

  static class TestCostFunction1 implements HasBrokerCost {

    public TestCostFunction1(Configuration configuration) {}

    static AtomicReference<Map<Integer, Collection<HasBeanObject>>> metrics =
        new AtomicReference<>();

    static HasBeanObject bean1 = () -> new BeanObject("Bean1", Map.of(), Map.of());

    @Override
    public Optional<Fetcher> fetcher() {
      return Optional.of((ignore) -> List.of(bean1));
    }

    @Override
    public BrokerCost brokerCost(ClusterInfo clusterInfo) {
      metrics.set(clusterInfo.clusterBean().all());
      return () ->
          clusterInfo.nodes().stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      NodeInfo::id, x -> ThreadLocalRandom.current().nextDouble(0, 1)));
    }
  }

  @Test
  @DisplayName("Two different cost functions won't receive metrics requested by the other")
  void testCostFunctionOfferedWithItsOwnMetrics() {
    var topic = "BalancerTest_testCostFunction_" + Utils.randomString();
    try (Admin admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(3).numberOfReplicas((short) 3).create();
    }

    var extraConfig =
        Map.of(
            BalancerConfigs.BALANCER_COST_FUNCTIONS,
                String.join(
                    ",", TestCostFunction0.class.getName(), TestCostFunction1.class.getName()),
            BalancerConfigs.BALANCER_REBALANCE_PLAN_EXECUTOR, DummyExecutor.class.getName());
    withPredefinedScenario(
        Set.of(topic),
        extraConfig,
        (config, balancer) -> {
          // act
          balancer.run();

          // assert
          var cfm0 = TestCostFunction0.metrics.get();
          var cfm1 = TestCostFunction1.metrics.get();

          cfm0.values().stream()
              .flatMap(Collection::stream)
              .forEach(bean -> Assertions.assertEquals("Bean0", bean.beanObject().domainName()));
          cfm1.values().stream()
              .flatMap(Collection::stream)
              .forEach(bean -> Assertions.assertEquals("Bean1", bean.beanObject().domainName()));
        });
  }
}
