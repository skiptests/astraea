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
package org.astraea.app.balancer.metrics;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.management.remote.JMXServiceURL;
import org.astraea.app.balancer.BalancerConfigs;
import org.astraea.app.common.Utils;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.jmx.MBeanClient;
import org.astraea.app.partitioner.Configuration;

public class JmxMetricSampler implements MetricSource {

  /**
   * The number of old time series to keep in data structure. note that this is not a strict upper
   * limit. The actual size might exceed. This issue is minor and fixing that might cause
   * performance issue. So no. This number must be larger than zero.
   */
  private final int queueSize;

  private final int brokerCount;
  private final int warmUpCount;
  private final LongAdder sampleCounter;

  private final Map<Integer, JMXServiceURL> jmxServiceURLMap;
  private final Map<Integer, MBeanClient> mBeanClientMap;
  private final Collection<IdentifiedFetcher> fetchers;
  private final Map<IdentifiedFetcher, Map<Integer, ConcurrentLinkedQueue<HasBeanObject>>> metrics;
  private final ScheduledExecutorService executorService;
  private final AtomicBoolean closed;
  private final Duration fetchInterval;
  private final List<ScheduledFuture<?>> scheduledFutures;

  private static Map<Integer, ConcurrentLinkedQueue<HasBeanObject>> newMetricStore(
      Set<Integer> brokerId) {
    return brokerId.stream()
        .collect(Collectors.toUnmodifiableMap(x -> x, x -> new ConcurrentLinkedQueue<>()));
  }

  public JmxMetricSampler(Configuration configuration, Collection<IdentifiedFetcher> fetchers) {
    var balancerConfigs = new BalancerConfigs(configuration);
    this.brokerCount = balancerConfigs.jmxServers().size();
    this.queueSize = balancerConfigs.metricScrapingQueueSize();
    this.warmUpCount = balancerConfigs.metricWarmUpCount();
    this.sampleCounter = new LongAdder();
    this.jmxServiceURLMap = Map.copyOf(balancerConfigs.jmxServers());
    this.mBeanClientMap =
        jmxServiceURLMap.entrySet().stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    Map.Entry::getKey, entry -> MBeanClient.of(entry.getValue())));
    this.fetchers = fetchers;
    this.metrics =
        fetchers.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    (identifiedFetcher) -> newMetricStore(jmxServiceURLMap.keySet())));
    this.executorService = Executors.newScheduledThreadPool(brokerCount);
    this.closed = new AtomicBoolean(false);
    this.fetchInterval = balancerConfigs.metricScrapingInterval();

    // schedule metric sampling tasks
    this.scheduledFutures =
        this.mBeanClientMap.entrySet().stream()
            .map(
                entry ->
                    this.executorService.scheduleAtFixedRate(
                        () -> {
                          int broker = entry.getKey();
                          var client = entry.getValue();
                          for (IdentifiedFetcher identifiedFetcher : fetchers) {
                            var metricStore = metrics.get(identifiedFetcher).get(broker);

                            // There is an issue related to Fetcher, the fetcher can fetch nothing
                            // back. So some queue might never growth.
                            metricStore.addAll(identifiedFetcher.fetch(client));

                            // draining old metrics
                            while (metricStore.size() > queueSize) metricStore.poll();
                          }
                          sampleCounter.increment();
                        },
                        0,
                        fetchInterval.toMillis(),
                        TimeUnit.MILLISECONDS))
            .collect(Collectors.toList());
  }

  private boolean ensureNotClosed() {
    if (closed.get()) throw new IllegalStateException("This metric source has been closed");
    return true;
  }

  @Override
  public Collection<HasBeanObject> metrics(IdentifiedFetcher fetcher, int brokerId) {
    ensureNotClosed();
    return Collections.unmodifiableCollection(metrics.get(fetcher).get(brokerId));
  }

  @Override
  public Map<Integer, Collection<HasBeanObject>> metrics(
      Set<Integer> nodes, IdentifiedFetcher fetcher) {
    ensureNotClosed();
    return MetricSource.super.metrics(nodes, fetcher);
  }

  @Override
  public Map<IdentifiedFetcher, Map<Integer, Collection<HasBeanObject>>> allBeans() {
    ensureNotClosed();
    return metrics.entrySet().stream()
        .collect(
            Collectors.toUnmodifiableMap(
                Map.Entry::getKey,
                x ->
                    metrics.get(x.getKey()).entrySet().stream()
                        .collect(
                            Collectors.toUnmodifiableMap(
                                Map.Entry::getKey,
                                y ->
                                    Collections.unmodifiableCollection(
                                        metrics.get(x.getKey()).get(y.getKey()))))));
  }

  @Override
  public double warmUpProgress() {
    ensureNotClosed();
    return Math.min(1.0, sampleCounter.doubleValue() / brokerCount / warmUpCount);
  }

  @Override
  public void awaitMetricReady() {
    ensureNotClosed();
    // TODO: the below close check won't do anything. waitFor will shallow the ensureNotClosed
    // error, cause caller wait for days even it is already closed
    Supplier<Boolean> allQueuesReady = () -> ensureNotClosed() && warmUpProgress() == 1.0;

    Utils.waitFor(allQueuesReady, ChronoUnit.DAYS.getDuration());
  }

  @Override
  public void drainMetrics() {
    ensureNotClosed();
    metrics.values().stream()
        .map(Map::values)
        .flatMap(Collection::stream)
        .forEach(ConcurrentLinkedQueue::clear);
  }

  @Override
  public void close() {
    // avoid being closed twice
    if (this.closed.getAndSet(true)) return;

    this.scheduledFutures.forEach(x -> x.cancel(true));
    this.scheduledFutures.clear();
    this.executorService.shutdown();
    this.mBeanClientMap.values().forEach(MBeanClient::close);
    this.metrics.values().stream()
        .map(Map::values)
        .flatMap(Collection::stream)
        .forEach(ConcurrentLinkedQueue::clear);
  }
}
