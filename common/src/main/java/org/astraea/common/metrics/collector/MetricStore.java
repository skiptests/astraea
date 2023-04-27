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
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.consumer.Deserializer;
import org.astraea.common.consumer.Record;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.MBeanClient;

public interface MetricStore extends AutoCloseable {

  static Builder builder() {
    return new Builder();
  }

  /**
   * @return the {@link ClusterBean} is composed by a bunch of metrics generated by {@link
   *     MetricSensor}
   */
  ClusterBean clusterBean();

  /**
   * @return the latest fetched identities
   */
  Set<Integer> identities();

  /**
   * @return the last used sensors
   */
  Map<MetricSensor, BiConsumer<Integer, Exception>> sensors();

  @Override
  void close();

  interface Receiver extends AutoCloseable {

    static MetricFetcher.Sender local() {
      return LocalSenderReceiver.of();
    }

    static Receiver topic(String bootstrapServer) {
      String METRIC_TOPIC = "__metrics";
      var consumer =
          Consumer.forTopics(Set.of(METRIC_TOPIC))
              .bootstrapServers(bootstrapServer)
              .config(
                  ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                  ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
              .keyDeserializer(Deserializer.INTEGER)
              .valueDeserializer(Deserializer.BEAN_OBJECT)
              .build();
      return new Receiver() {
        @Override
        public Map<Integer, Collection<BeanObject>> receive(Duration timeout) {
          return consumer.poll(timeout).stream()
              .collect(
                  Collectors.groupingBy(
                      Record::key,
                      Collectors.mapping(Record::value, Collectors.toCollection(ArrayList::new))));
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
    private Supplier<Map<MetricSensor, BiConsumer<Integer, Exception>>> sensorsSupplier =
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

    public Builder sensorsSupplier(
        Supplier<Map<MetricSensor, BiConsumer<Integer, Exception>>> sensorsSupplier) {
      this.sensorsSupplier = sensorsSupplier;
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
    public Builder localReceiver(
        Supplier<CompletionStage<Map<Integer, MBeanClient>>> clientSupplier) {
      var cache = LocalSenderReceiver.of();
      var fetcher = MetricFetcher.builder().clientSupplier(clientSupplier).sender(cache).build();
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

    public MetricStore build() {
      return new MetricStoreImpl(
          Objects.requireNonNull(sensorsSupplier, "sensorsSupplier can't be null"),
          Objects.requireNonNull(receiver, "receiver can't be null"),
          Objects.requireNonNull(beanExpiration, "beanExpiration can't be null"));
    }
  }

  class MetricStoreImpl implements MetricStore {

    private final Map<Integer, Collection<HasBeanObject>> beans = new ConcurrentHashMap<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final Receiver receiver;

    private final ExecutorService executor;

    // cache the latest cluster to be shared between all threads.
    private volatile ClusterBean lastClusterBean = ClusterBean.EMPTY;

    // trace the identities of returned metrics
    private final Set<Integer> identities = new ConcurrentSkipListSet<>();

    private volatile Map<MetricSensor, BiConsumer<Integer, Exception>> lastSensors = Map.of();

    private MetricStoreImpl(
        Supplier<Map<MetricSensor, BiConsumer<Integer, Exception>>> sensorsSupplier,
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
                identities.addAll(allBeans.keySet());
                lastSensors = sensorsSupplier.get();
                allBeans.forEach(
                    (id, bs) -> {
                      var client = MBeanClient.of(bs);
                      var clusterBean = clusterBean();
                      lastSensors.forEach(
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
      return lastClusterBean;
    }

    @Override
    public Set<Integer> identities() {
      return Set.copyOf(identities);
    }

    @Override
    public Map<MetricSensor, BiConsumer<Integer, Exception>> sensors() {
      return Map.copyOf(lastSensors);
    }

    @Override
    public void close() {
      closed.set(true);
      executor.shutdownNow();
      Utils.packException(() -> executor.awaitTermination(30, TimeUnit.SECONDS));
      receiver.close();
    }

    private void updateClusterBean() {
      lastClusterBean =
          ClusterBean.of(
              beans.entrySet().stream()
                  .filter(entry -> !entry.getValue().isEmpty())
                  .collect(
                      Collectors.toUnmodifiableMap(
                          Map.Entry::getKey, e -> List.copyOf(e.getValue()))));
    }
  }
}
