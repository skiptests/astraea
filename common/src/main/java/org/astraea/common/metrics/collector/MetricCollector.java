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
package org.astraea.common.metrics.collector;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.metrics.HasBeanObject;

public interface MetricCollector extends AutoCloseable {

  /**
   * Register a {@link MetricSensor}.
   *
   * <p>Note that sensors will be used by every identity. It is possible that the metric this {@link
   * MetricSensor} is sampling doesn't exist on a JMX server(For example: sampling Kafka broker
   * metric from a Producer client). When such case occurred. The {@code noSuchMetricHandler} will
   * be invoked.
   *
   * @param metricSensor the sensor
   * @param noSuchMetricHandler call this if the sensor raise a {@link
   *     java.util.NoSuchElementException} exception. The first argument is the identity number. The
   *     second argument is the exception itself.
   */
  void addMetricSensor(
      MetricSensor metricSensor, BiConsumer<Integer, Exception> noSuchMetricHandler);

  /**
   * Register a {@link MetricSensor}.
   *
   * <p>This method swallow the exception caused by {@link java.util.NoSuchElementException}. For
   * further detail see {@link MetricCollector#addMetricSensor(MetricSensor, BiConsumer)}.
   *
   * @see MetricCollector#addMetricSensor(MetricSensor, BiConsumer)
   * @param metricSensor the sensor
   */
  default void addMetricSensor(MetricSensor metricSensor) {
    addMetricSensor(metricSensor, (i0, i1) -> {});
  }

  /** Register a JMX server. */
  void registerJmx(int identity, InetSocketAddress socketAddress);

  /** Register the JMX server on this JVM instance. */
  void registerLocalJmx(int identity);

  /**
   * @return the current registered sensors.
   */
  Collection<MetricSensor> metricSensors();

  /**
   * @return the current registered identities.
   */
  Set<Integer> listIdentities();

  /**
   * @return the type of metrics has been sampled so far.
   */
  Set<Class<? extends HasBeanObject>> listMetricTypes();

  /**
   * @return size of stored beans
   */
  int size();

  /**
   * @return a weak consistency stream for stored beans.
   */
  Stream<HasBeanObject> metrics();

  /**
   * @return a weak consistency stream for stored beans.
   */
  default <T extends HasBeanObject> Stream<T> metrics(Class<T> clz) {
    return metrics().filter(b -> clz.isAssignableFrom(b.getClass())).map(clz::cast);
  }

  /**
   * @return the {@link ClusterBean}.
   */
  ClusterBean clusterBean();

  @Override
  void close();

  static MetricCollectorImpl.Builder builder() {
    return new MetricCollectorImpl.Builder();
  }
}
