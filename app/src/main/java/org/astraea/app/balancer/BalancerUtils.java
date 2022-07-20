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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.ReplicaInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.balancer.executor.RebalancePlanExecutor;
import org.astraea.app.balancer.generator.RebalancePlanGenerator;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.log.LayeredClusterLogAllocation;
import org.astraea.app.balancer.metrics.IdentifiedFetcher;
import org.astraea.app.balancer.metrics.MetricSource;
import org.astraea.app.cost.CostFunction;
import org.astraea.app.partitioner.Configuration;

class BalancerUtils {

  /**
   * Create a {@link ClusterInfo} with its log placement replaced by {@link ClusterLogAllocation}.
   * Every log will be marked as online & synced. Based on the given content in {@link
   * ClusterLogAllocation}, some logs might not have its data directory specified. Noted that this
   * method doesn't check if the given logs is suitable & exists in the cluster info base. the beans
   * alongside the based cluster info might be out-of-date or even completely meaningless.
   *
   * @param clusterInfo the based cluster info
   * @param allocation the log allocation to replace {@link ClusterInfo}'s log placement. If the
   *     allocation implementation is {@link LayeredClusterLogAllocation} then the given instance
   *     will be locked.
   * @return a {@link ClusterInfo} with its log placement replaced.
   */
  public static ClusterInfo mockClusterInfoAllocation(
      ClusterInfo clusterInfo, ClusterLogAllocation allocation) {
    // making defensive copy
    final var allocationCopy = LayeredClusterLogAllocation.of(allocation);
    return new ClusterInfo() {
      // TODO: maybe add a field to tell if this cluster info is mocked.

      @Override
      public List<NodeInfo> nodes() {
        return clusterInfo.nodes();
      }

      @Override
      public Set<String> dataDirectories(int brokerId) {
        return clusterInfo.dataDirectories(brokerId);
      }

      @Override
      public Set<String> topics() {
        return allocationCopy
            .topicPartitionStream()
            .map(TopicPartition::topic)
            .collect(Collectors.toUnmodifiableSet());
      }

      @Override
      public List<ReplicaInfo> availableReplicaLeaders(String topic) {
        return replicas(topic).stream()
            .filter(ReplicaInfo::isLeader)
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public List<ReplicaInfo> availableReplicas(String topic) {
        // there is no offline sense for a fake cluster info, so everything is online.
        return replicas(topic);
      }

      @Override
      public List<ReplicaInfo> replicas(String topic) {
        Map<Integer, NodeInfo> nodeIdMap =
            nodes().stream()
                .collect(Collectors.toUnmodifiableMap(NodeInfo::id, Function.identity()));
        var result =
            allocationCopy
                .topicPartitionStream()
                .filter(tp -> tp.topic().equals(topic))
                .map(tp -> Map.entry(tp, allocationCopy.logPlacements(tp)))
                .flatMap(
                    entry -> {
                      var tp = entry.getKey();
                      var logs = entry.getValue();

                      return IntStream.range(0, logs.size())
                          .mapToObj(
                              i ->
                                  ReplicaInfo.of(
                                      tp.topic(),
                                      tp.partition(),
                                      nodeIdMap.get(logs.get(i).broker()),
                                      i == 0,
                                      true,
                                      false,
                                      logs.get(i).logDirectory().orElse(null)));
                    })
                .collect(Collectors.toUnmodifiableList());

        if (result.isEmpty()) throw new NoSuchElementException();

        return result;
      }

      @Override
      public ClusterBean clusterBean() {
        return clusterInfo.clusterBean();
      }
    };
  }

  private static boolean crossCheck(Class<?> a, Class<?> b, Class<?> c, Class<?> d) {
    return (a == c && b == d) || (a == d && b == c);
  }

  /** Construct an instance of given class, with given arguments as the constructor argument */
  static <T> Optional<T> newInstance(Class<? extends T> aClass, Object... args) {
    try {
      // Class#getConstructor(Class<?> argTypes) doesn't consider the inheritance relationship.
      // Which means `MyClass(Number)` constructor must give a variable with the exact same type
      // `Number`. Given an `Integer`, `Double` or any anonymous class is not going to work. Also,
      // `Number` is an abstract class so there is basically no variable can match this constructor.
      // Given that abstract class must be initialized with a concrete implementation. By the time
      // we provide a concrete implementation, it is no longer that `Number` type. The same as
      // interface. This makes the `Configuration` type(interface) impossible to search by that
      // method. To bypass this issue we have to use Class#getConstructors(), then manually check
      // each constructor and validate the subclass assignment relationship all by ourselves.
      @SuppressWarnings("unchecked") // see javadoc of Class#getConstructors() for the reason.
      var constructors = (Constructor<T>[]) aClass.getConstructors();

      // deal with primitive type. The API doesn't consider int assignable to Integer
      var isAssignable =
          (BiFunction<Class<?>, Class<?>, Boolean>)
              (c0, c1) ->
                  c0.isAssignableFrom(c1)
                      || crossCheck(c0, c1, byte.class, Byte.class)
                      || crossCheck(c0, c1, long.class, Long.class)
                      || crossCheck(c0, c1, short.class, Short.class)
                      || crossCheck(c0, c1, int.class, Integer.class)
                      || crossCheck(c0, c1, float.class, Float.class)
                      || crossCheck(c0, c1, double.class, Double.class)
                      || crossCheck(c0, c1, char.class, Character.class)
                      || crossCheck(c0, c1, boolean.class, Boolean.class);

      // list of user given arguments for constructor
      var given = Arrays.stream(args).map(Object::getClass).toArray(Class<?>[]::new);

      // find a suitable constructor
      for (var constructor : constructors) {
        var actual = constructor.getParameterTypes();

        if (given.length == actual.length) {
          boolean allAssignable =
              IntStream.range(0, actual.length)
                  .allMatch(i -> isAssignable.apply(actual[i], given[i]));
          if (allAssignable) return Optional.of(constructor.newInstance(args));
        }
      }
    } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    return Optional.empty();
  }

  static Supplier<RuntimeException> noSuitableConstructorException(Class<?> theClass) {
    return () ->
        new IllegalArgumentException(
            "No suitable class constructor found for " + theClass.getName());
  }

  static MetricSource constructMetricSource(
      Class<? extends MetricSource> aClass,
      Configuration configuration,
      Collection<IdentifiedFetcher> fetchers) {
    return newInstance(aClass, configuration, fetchers)
        .orElseThrow(noSuitableConstructorException(aClass));
  }

  static CostFunction constructCostFunction(
      Class<? extends CostFunction> aClass, Configuration configuration) {
    return Stream.of(newInstance(aClass, configuration), newInstance(aClass))
        .flatMap(Optional::stream)
        .findFirst()
        .orElseThrow(noSuitableConstructorException(aClass));
  }

  static RebalancePlanGenerator constructGenerator(
      Class<? extends RebalancePlanGenerator> aClass, Configuration configuration) {
    return Stream.of(newInstance(aClass, configuration), newInstance(aClass))
        .flatMap(Optional::stream)
        .findFirst()
        .orElseThrow(noSuitableConstructorException(aClass));
  }

  static RebalancePlanExecutor constructExecutor(
      Class<? extends RebalancePlanExecutor> aClass, Configuration configuration) {
    return Stream.of(newInstance(aClass, configuration), newInstance(aClass))
        .flatMap(Optional::stream)
        .findFirst()
        .orElseThrow(noSuitableConstructorException(aClass));
  }
}
