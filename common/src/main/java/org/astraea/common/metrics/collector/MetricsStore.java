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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.consumer.Deserializer;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.MBeanClient;

public interface MetricsStore extends AutoCloseable {

  static Builder builder() {
    return new Builder();
  }

  ClusterBean clusterBean();

  @Override
  void close();

  interface Receiver extends AutoCloseable {
    Pattern INTERNAL_TOPIC_PATTERN = Pattern.compile("__(?<brokerId>[0-9]+)_broker_metrics");

    static MetricsFetcher.Sender local() {
      return LocalSenderReceiver.of();
    }

    static Receiver topic(String bootstrapServer) {
      var consumer =
          Consumer.forTopics(INTERNAL_TOPIC_PATTERN)
              .bootstrapServers(bootstrapServer)
              .config(
                  ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                  ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
              .valueDeserializer(Deserializer.BEAN_OBJECT)
              .build();
      return new Receiver() {
        @Override
        public Map<Integer, Collection<BeanObject>> receive(Duration timeout) {
          return consumer.poll(timeout).stream()
              .filter(r -> r.value() != null)
              // Parsing topic name
              .map(r -> Map.entry(INTERNAL_TOPIC_PATTERN.matcher(r.topic()), r.value()))
              .filter(matcherBean -> matcherBean.getKey().matches())
              .collect(
                  Collectors.groupingBy(
                      matcherBean -> Integer.parseInt(matcherBean.getKey().group("brokerId")),
                      Collectors.mapping(
                          Map.Entry::getValue, Collectors.toCollection(ArrayList::new))));
        }

        @Override
        public void close() {
          consumer.close();
        }
      };
    }

    Map<Integer, Collection<BeanObject>> receive(Duration timeout);

    @Override
    default void close() {}
  }

  class Builder {

    // default impl returns all input metrics
    private Supplier<Map<MetricSensor, BiConsumer<Integer, Exception>>> sensorSupplier =
        () ->
            Map.of(
                (MetricSensor)
                    (client, bean) ->
                        client.beans(BeanQuery.all()).stream()
                            .map(bs -> (HasBeanObject) () -> bs)
                            .collect(Collectors.toUnmodifiableList()),
                (id, ignored) -> {});

    private Receiver receiver;
    private Duration beanExpiration = Duration.ofSeconds(10);

    public Builder sensorSupplier(
        Supplier<Map<MetricSensor, BiConsumer<Integer, Exception>>> sensorSupplier) {
      this.sensorSupplier = sensorSupplier;
      return this;
    }

    public Builder receiver(Receiver receiver) {
      this.receiver = receiver;
      return this;
    }

    /**
     * Using an embedded fetcher build the receiver. The fetcher will keep fetching beans
     * background, and it pushes all beans to store internally.
     */
    public Builder localReceiver(Supplier<Map<Integer, MBeanClient>> clientSupplier) {
      var cache = LocalSenderReceiver.of();
      var fetcher = MetricsFetcher.builder().clientSupplier(clientSupplier).sender(cache).build();
      return receiver(
          new Receiver() {
            @Override
            public Map<Integer, Collection<BeanObject>> receive(Duration timeout) {
              return cache.receive(timeout);
            }

            @Override
            public void close() {
              fetcher.close();
            }
          });
    }

    public Builder topicReceiver(String bootstrapServer) {
      return receiver(Receiver.topic(bootstrapServer));
    }

    public Builder beanExpiration(Duration beanExpiration) {
      this.beanExpiration = beanExpiration;
      return this;
    }

    public MetricsStore build() {
      return new MetricsStoreImpl(
          Objects.requireNonNull(sensorSupplier, "sensorSupplier can't be null"),
          Objects.requireNonNull(receiver, "receiver can't be null"),
          Objects.requireNonNull(beanExpiration, "beanExpiration can't be null"));
    }
  }

  class MetricsStoreImpl implements MetricsStore {

    private final Map<Integer, Collection<HasBeanObject>> beans = new ConcurrentHashMap<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final Receiver receiver;

    private final ExecutorService executor;

    // cache the latest cluster to be shared between all threads.
    private volatile ClusterBean latest = ClusterBean.EMPTY;

    private MetricsStoreImpl(
        Supplier<Map<MetricSensor, BiConsumer<Integer, Exception>>> sensorSupplier,
        Receiver receiver,
        Duration beanExpiration) {
      this.receiver = receiver;
      // receiver + cleaner
      this.executor = Executors.newFixedThreadPool(2);
      Runnable cleanerJob =
          () -> {
            while (!closed.get()) {
              try {
                var before = System.currentTimeMillis() - beanExpiration.toMillis();
                var noUpdate =
                    this.beans.values().stream()
                        .noneMatch(
                            bs ->
                                bs.removeIf(
                                    hasBeanObject -> hasBeanObject.createdTimestamp() < before));
                if (!noUpdate) updateClusterBean();
                TimeUnit.MILLISECONDS.sleep(beanExpiration.toMillis());
              } catch (Exception e) {
                // TODO: it needs better error handling
                e.printStackTrace();
              }
            }
          };
      Runnable receiverJob =
          () -> {
            while (!closed.get()) {
              try {
                var allBeans = receiver.receive(Duration.ofSeconds(3));
                allBeans.forEach(
                    (id, bs) -> {
                      var client = MBeanClient.of(bs);
                      var clusterBean = clusterBean();
                      sensorSupplier
                          .get()
                          .forEach(
                              (sensor, errorHandler) -> {
                                try {
                                  beans
                                      .computeIfAbsent(id, ignored -> new ConcurrentLinkedQueue<>())
                                      .addAll(sensor.fetch(client, clusterBean));
                                } catch (Exception e) {
                                  errorHandler.accept(id, e);
                                }
                              });
                    });
                // generate new cluster bean
                if (!allBeans.isEmpty()) updateClusterBean();
              } catch (Exception e) {
                // TODO: it needs better error handling
                e.printStackTrace();
              }
            }
          };
      executor.execute(cleanerJob);
      executor.execute(receiverJob);
    }

    @Override
    public ClusterBean clusterBean() {
      return latest;
    }

    @Override
    public void close() {
      closed.set(true);
      executor.shutdownNow();
      Utils.packException(() -> executor.awaitTermination(30, TimeUnit.SECONDS));
      receiver.close();
    }

    private void updateClusterBean() {
      latest =
          ClusterBean.of(
              beans.entrySet().stream()
                  .filter(entry -> !entry.getValue().isEmpty())
                  .collect(
                      Collectors.toUnmodifiableMap(
                          Map.Entry::getKey, e -> List.copyOf(e.getValue()))));
    }
  }
}