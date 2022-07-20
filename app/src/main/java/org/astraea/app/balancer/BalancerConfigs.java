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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.management.remote.JMXServiceURL;
import org.astraea.app.balancer.executor.RebalancePlanExecutor;
import org.astraea.app.balancer.executor.StraightPlanExecutor;
import org.astraea.app.balancer.generator.RebalancePlanGenerator;
import org.astraea.app.balancer.generator.ShufflePlanGenerator;
import org.astraea.app.balancer.metrics.JmxMetricSampler;
import org.astraea.app.balancer.metrics.MetricSource;
import org.astraea.app.common.Utils;
import org.astraea.app.cost.CostFunction;
import org.astraea.app.cost.CpuCost;
import org.astraea.app.partitioner.Configuration;

public class BalancerConfigs implements Configuration {

  public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
  public static final String JMX_SERVERS_CONFIG = "jmx.servers";
  public static final String METRICS_SCRAPING_QUEUE_SIZE_CONFIG = "metrics.scraping.queue.size";
  public static final String METRICS_SCRAPING_INTERVAL_MS_CONFIG = "metrics.scraping.interval.ms";
  public static final String METRICS_WARM_UP_COUNT_CONFIG = "metrics.warm.up.count";
  public static final String BALANCER_ALLOWED_TOPICS = "balancer.allowed.topics";
  public static final String BALANCER_IGNORED_TOPICS_CONFIG = "balancer.ignored.topics";
  public static final String BALANCER_METRIC_SOURCE_CLASS = "balancer.metric.source.class";
  public static final String BALANCER_COST_FUNCTIONS = "balancer.cost.functions";
  public static final String BALANCER_REBALANCE_PLAN_GENERATOR =
      "balancer.rebalance.plan.generator";
  public static final String BALANCER_REBALANCE_PLAN_EXECUTOR = "balancer.rebalance.plan.executor";
  public static final String BALANCER_PLAN_SEARCHING_ITERATION =
      "balancer.plan.searching.iteration";
  public static final String BALANCER_RUN_COUNT = "balancer.run.count";

  private final Configuration configuration;

  public BalancerConfigs(Configuration configuration) {
    this.configuration = configuration;
  }

  /** Ensure the configuration is offered correctly. */
  public void sanityCheck() {
    List<Method> methods = List.of(this.getClass().getMethods());
    methods.stream()
        .filter(m -> m.getAnnotation(Config.class) != null)
        .filter(m -> m.getAnnotation(Required.class) != null)
        .forEach(m -> requireString(m.getAnnotation(Config.class).key()));
    methods.stream()
        .filter(m -> m.getAnnotation(Config.class) != null)
        .forEach(
            m -> {
              try {
                // TODO: verify config values are in valid range
                Objects.requireNonNull(m.invoke(this));
              } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
              } catch (Exception e) {
                var key = m.getAnnotation(Config.class).key();
                throw new IllegalArgumentException(
                    "Failed to fetch required configuration " + m.getName() + " \"" + key + "\"",
                    e);
              }
            });
  }

  @Config(key = BOOTSTRAP_SERVERS_CONFIG)
  @Required
  public String bootstrapServers() {
    return requireString(BOOTSTRAP_SERVERS_CONFIG);
  }

  @Config(key = JMX_SERVERS_CONFIG)
  @Required
  public Map<Integer, JMXServiceURL> jmxServers() {
    return map(
        JMX_SERVERS_CONFIG,
        ",",
        "@",
        Integer::parseInt,
        (s) -> Utils.packException(() -> new JMXServiceURL(s)));
  }

  @Config(key = METRICS_SCRAPING_QUEUE_SIZE_CONFIG)
  public int metricScrapingQueueSize() {
    int val = string(METRICS_SCRAPING_QUEUE_SIZE_CONFIG).map(Integer::parseInt).orElse(300);
    assertion("value should be positive integer", val > 0);
    return val;
  }

  @Config(key = METRICS_SCRAPING_INTERVAL_MS_CONFIG)
  public Duration metricScrapingInterval() {
    var val =
        string(METRICS_SCRAPING_INTERVAL_MS_CONFIG)
            .map(Integer::parseInt)
            .map(Duration::ofMillis)
            .orElse(Duration.ofSeconds(1));
    assertion("interval should be non-negative integer", !val.isNegative());
    return val;
  }

  @Config(key = METRICS_WARM_UP_COUNT_CONFIG)
  public int metricWarmUpCount() {
    int val = string(METRICS_WARM_UP_COUNT_CONFIG).map(Integer::parseInt).orElse(30);
    assertion("value should be non-negative integer", val >= 0);
    return val;
  }

  @Config(key = BALANCER_IGNORED_TOPICS_CONFIG)
  public Set<String> ignoredTopics() {
    return string(BALANCER_IGNORED_TOPICS_CONFIG).stream()
        .flatMap(s -> Arrays.stream(s.split(",")))
        .filter(x -> !x.isEmpty())
        .collect(Collectors.toUnmodifiableSet());
  }

  @Config(key = BALANCER_ALLOWED_TOPICS)
  public Set<String> allowedTopics() {
    return string(BALANCER_ALLOWED_TOPICS).stream()
        .flatMap(s -> Arrays.stream(s.split(",")))
        .filter(x -> !x.isEmpty())
        .collect(Collectors.toUnmodifiableSet());
  }

  @Config(key = BALANCER_COST_FUNCTIONS)
  public List<Class<? extends CostFunction>> costFunctionClasses() {
    var defaultValue =
        Stream.of(CpuCost.class).map(Class::getName).collect(Collectors.joining(","));

    return Stream.of(
            string(BALANCER_COST_FUNCTIONS)
                .filter(x -> !x.isEmpty())
                .orElse(defaultValue)
                .split(","))
        .map(classname -> resolveClass(classname, CostFunction.class))
        .collect(Collectors.toUnmodifiableList());
  }

  @Config(key = BALANCER_REBALANCE_PLAN_GENERATOR)
  public Class<? extends RebalancePlanGenerator> rebalancePlanGeneratorClass() {
    String classname =
        string(BALANCER_REBALANCE_PLAN_GENERATOR).orElse(ShufflePlanGenerator.class.getName());
    return resolveClass(classname, RebalancePlanGenerator.class);
  }

  @Config(key = BALANCER_REBALANCE_PLAN_EXECUTOR)
  public Class<? extends RebalancePlanExecutor> rebalancePlanExecutorClass() {
    String classname =
        string(BALANCER_REBALANCE_PLAN_EXECUTOR).orElse(StraightPlanExecutor.class.getName());
    return resolveClass(classname, RebalancePlanExecutor.class);
  }

  @Config(key = BALANCER_PLAN_SEARCHING_ITERATION)
  public int rebalancePlanSearchingIteration() {
    int val = string(BALANCER_PLAN_SEARCHING_ITERATION).map(Integer::parseInt).orElse(2000);
    assertion("value should be a positive integer", val > 0);
    return val;
  }

  @Config(key = BALANCER_METRIC_SOURCE_CLASS)
  public Class<? extends MetricSource> metricSourceClass() {
    String classname =
        string(BALANCER_METRIC_SOURCE_CLASS).orElse(JmxMetricSampler.class.getName());
    return resolveClass(classname, MetricSource.class);
  }

  @Config(key = BALANCER_RUN_COUNT)
  public Integer balancerRunCount() {
    return string(BALANCER_RUN_COUNT).map(Integer::parseInt).orElse(Integer.MAX_VALUE);
  }

  private static <T> Class<T> resolveClass(String classname, Class<T> extendedType) {
    Class<?> aClass = Utils.packException(() -> Class.forName(classname));

    if (extendedType.isAssignableFrom(aClass)) {
      @SuppressWarnings("unchecked")
      var theClass = (Class<T>) aClass;
      return theClass;
    } else {
      throw new IllegalArgumentException(
          "The given class \"" + classname + "\" is not a instance of " + extendedType.getName());
    }
  }

  public Configuration asConfiguration() {
    return configuration;
  }

  public void assertion(String description, boolean condition) {
    if (!condition) throw new IllegalArgumentException(description);
  }

  @Override
  public Optional<String> string(String key) {
    return configuration.string(key);
  }

  @Override
  public List<String> list(String key, String separator) {
    return configuration.list(key, separator);
  }

  @Override
  public <K, V> Map<K, V> map(
      String key,
      String listSeparator,
      String mapSeparator,
      Function<String, K> keyConverter,
      Function<String, V> valueConverter) {
    return configuration.map(key, listSeparator, mapSeparator, keyConverter, valueConverter);
  }

  @Override
  public Set<Map.Entry<String, String>> entrySet() {
    return configuration.entrySet();
  }

  // visible for test
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @interface Required {}

  // visible for test
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @interface Config {
    String key();
  }
}
