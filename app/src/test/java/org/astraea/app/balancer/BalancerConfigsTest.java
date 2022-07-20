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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.management.remote.JMXServiceURL;
import org.astraea.app.balancer.utils.DummyCostFunction;
import org.astraea.app.balancer.utils.DummyExecutor;
import org.astraea.app.balancer.utils.DummyGenerator;
import org.astraea.app.balancer.utils.DummyMetricSource;
import org.astraea.app.common.Utils;
import org.astraea.app.partitioner.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BalancerConfigsTest {

  public static String jndiString(String host, int port) {
    return String.format("service:jmx:rmi://%s:%d/jndi/rmi://%s:%d/jmxrmi", host, port, host, port);
  }

  public static String jmxString(int brokerId, String hostname, int port) {
    return String.format("%d@%s", brokerId, jndiString(hostname, port));
  }

  private static class TestData {
    public static final String valueJmx =
        String.join(
            ",",
            jmxString(1001, "host1", 5566),
            jmxString(1002, "host2", 5566),
            jmxString(1003, "host3", 5566));

    public static final Map<Integer, JMXServiceURL> expJmx =
        Utils.packException(
            () ->
                Map.of(
                    1001,
                        new JMXServiceURL(
                            "service:jmx:rmi://host1:5566/jndi/rmi://host1:5566/jmxrmi"),
                    1002,
                        new JMXServiceURL(
                            "service:jmx:rmi://host2:5566/jndi/rmi://host2:5566/jmxrmi"),
                    1003,
                        new JMXServiceURL(
                            "service:jmx:rmi://host3:5566/jndi/rmi://host3:5566/jmxrmi")));

    public static final String valueCostFunctions =
        String.join(",", DummyCostFunction.class.getName(), DummyCostFunction.class.getName());
    public static final List<Class<?>> expCostFunctions =
        List.of(DummyCostFunction.class, DummyCostFunction.class);

    public static final String valueGenerator = DummyGenerator.class.getName();
    public static final Class<?> expGenerator = DummyGenerator.class;

    public static final String valueExecutor = DummyExecutor.class.getName();
    public static final Class<?> expExecutor = DummyExecutor.class;

    public static final String valueMetricSource = DummyMetricSource.class.getName();
    public static final String valueMetricSources =
        String.join(",", DummyMetricSource.class.getName(), DummyMetricSource.class.getName());
    public static final Class<?> expMetricSource = DummyMetricSource.class;
  }

  static <T> Arguments passCase(String key, String value, T expectedReturn) {
    return Arguments.arguments(true, key, value, expectedReturn);
  }

  static Arguments failCase(String key, String value) {
    return Arguments.arguments(false, key, value, null);
  }

  static Stream<Arguments> testcases() {
    return Stream.of(
        passCase(BalancerConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker0:5566", "broker0:5566"),
        passCase(BalancerConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker1:5566", "broker1:5566"),
        passCase(BalancerConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker2:5566", "broker2:5566"),
        passCase(BalancerConfigs.JMX_SERVERS_CONFIG, TestData.valueJmx, TestData.expJmx),
        failCase(BalancerConfigs.JMX_SERVERS_CONFIG, "1001@localhost:5566"),
        passCase(BalancerConfigs.METRICS_SCRAPING_QUEUE_SIZE_CONFIG, "10", 10),
        failCase(BalancerConfigs.METRICS_SCRAPING_QUEUE_SIZE_CONFIG, "ten"),
        failCase(BalancerConfigs.METRICS_SCRAPING_QUEUE_SIZE_CONFIG, "-100"),
        failCase(BalancerConfigs.METRICS_SCRAPING_QUEUE_SIZE_CONFIG, "0"),
        passCase(BalancerConfigs.METRICS_SCRAPING_INTERVAL_MS_CONFIG, "10", Duration.ofMillis(10)),
        failCase(BalancerConfigs.METRICS_SCRAPING_INTERVAL_MS_CONFIG, "50cent"),
        failCase(BalancerConfigs.METRICS_SCRAPING_INTERVAL_MS_CONFIG, "-30"),
        passCase(BalancerConfigs.METRICS_WARM_UP_COUNT_CONFIG, "10", 10),
        failCase(BalancerConfigs.METRICS_WARM_UP_COUNT_CONFIG, "lOO"),
        failCase(BalancerConfigs.METRICS_WARM_UP_COUNT_CONFIG, "-50"),
        passCase(BalancerConfigs.BALANCER_IGNORED_TOPICS_CONFIG, "topic", Set.of("topic")),
        passCase(BalancerConfigs.BALANCER_IGNORED_TOPICS_CONFIG, "a,b,c", Set.of("a", "b", "c")),
        passCase(
            BalancerConfigs.BALANCER_COST_FUNCTIONS,
            TestData.valueCostFunctions,
            TestData.expCostFunctions),
        failCase(BalancerConfigs.BALANCER_COST_FUNCTIONS, Object.class.getName()),
        passCase(
            BalancerConfigs.BALANCER_REBALANCE_PLAN_GENERATOR,
            TestData.valueGenerator,
            TestData.expGenerator),
        failCase(BalancerConfigs.BALANCER_REBALANCE_PLAN_GENERATOR, Object.class.getName()),
        passCase(
            BalancerConfigs.BALANCER_REBALANCE_PLAN_EXECUTOR,
            TestData.valueExecutor,
            TestData.expExecutor),
        failCase(BalancerConfigs.BALANCER_REBALANCE_PLAN_EXECUTOR, Object.class.getName()),
        passCase(
            BalancerConfigs.BALANCER_METRIC_SOURCE_CLASS,
            TestData.valueMetricSource,
            TestData.expMetricSource),
        failCase(BalancerConfigs.BALANCER_METRIC_SOURCE_CLASS, TestData.valueMetricSources),
        failCase(BalancerConfigs.BALANCER_METRIC_SOURCE_CLASS, Object.class.getName()),
        passCase(BalancerConfigs.BALANCER_PLAN_SEARCHING_ITERATION, "3000", 3000),
        failCase(BalancerConfigs.BALANCER_PLAN_SEARCHING_ITERATION, "owo"),
        failCase(BalancerConfigs.BALANCER_PLAN_SEARCHING_ITERATION, "0"),
        failCase(BalancerConfigs.BALANCER_PLAN_SEARCHING_ITERATION, "-5"),
        passCase(
            BalancerConfigs.BALANCER_ALLOWED_TOPICS, "Aaa,B,C,D", Set.of("Aaa", "B", "C", "D")),
        passCase(BalancerConfigs.BALANCER_RUN_COUNT, "1024", 1024),
        failCase(BalancerConfigs.BALANCER_RUN_COUNT, "once"));
  }

  @ParameterizedTest
  @MethodSource(value = "testcases")
  void testConfig(boolean shouldPass, String key, String value, Object expectedReturn) {
    var config = Configuration.of(Map.of(key, value));
    var balancerConfig = new BalancerConfigs(config);
    Supplier<?> fetchConfig =
        () ->
            Utils.packException(
                () ->
                    Arrays.stream(BalancerConfigs.class.getMethods())
                        .filter(m -> m.getAnnotation(BalancerConfigs.Config.class) != null)
                        .filter(
                            m -> m.getAnnotation(BalancerConfigs.Config.class).key().equals(key))
                        .findFirst()
                        .orElseThrow()
                        .invoke(balancerConfig));

    if (shouldPass) {
      Assertions.assertEquals(expectedReturn, fetchConfig.get());
    } else {
      Assertions.assertThrows(Throwable.class, fetchConfig::get);
    }
  }

  @Test
  void ensureAllConfigsAreCoveredInTest() {

    Set<String> allConfigs =
        Arrays.stream(BalancerConfigs.class.getMethods())
            .filter(x -> x.getAnnotation(BalancerConfigs.Config.class) != null)
            .map(x -> x.getAnnotation(BalancerConfigs.Config.class).key())
            .collect(Collectors.toUnmodifiableSet());

    Set<String> testedConfigs =
        testcases().map(x -> (String) x.get()[1]).collect(Collectors.toUnmodifiableSet());

    // use TreeSet to ensure print order.
    Assertions.assertEquals(new TreeSet<>(allConfigs), new TreeSet<>(testedConfigs));
  }

  @Test
  void testSanityCheck() {
    var emptyConfigs = new BalancerConfigs(Configuration.of(Map.of()));
    Assertions.assertThrows(Exception.class, emptyConfigs::sanityCheck);

    var minimumConfigs =
        new BalancerConfigs(
            Configuration.of(
                Map.of(
                    BalancerConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    "",
                    BalancerConfigs.JMX_SERVERS_CONFIG,
                    jmxString(1001, "host0", 5566))));
    Assertions.assertDoesNotThrow(minimumConfigs::sanityCheck);

    var badConfigs =
        new BalancerConfigs(
            Configuration.of(
                Map.of(
                    BalancerConfigs.BOOTSTRAP_SERVERS_CONFIG, "",
                    BalancerConfigs.JMX_SERVERS_CONFIG, jmxString(1001, "host0", 5566),
                    BalancerConfigs.BALANCER_REBALANCE_PLAN_GENERATOR,
                        "com.example.class.not.found")));
    Assertions.assertThrows(IllegalArgumentException.class, badConfigs::sanityCheck);

    var invalidValueConfigs =
        new BalancerConfigs(
            Configuration.of(
                Map.of(
                    BalancerConfigs.BOOTSTRAP_SERVERS_CONFIG, "",
                    BalancerConfigs.JMX_SERVERS_CONFIG, jmxString(1001, "host0", 5566),
                    BalancerConfigs.BALANCER_PLAN_SEARCHING_ITERATION, "0")));
    Assertions.assertThrows(IllegalArgumentException.class, invalidValueConfigs::sanityCheck);
  }
}
