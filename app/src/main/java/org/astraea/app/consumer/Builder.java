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
package org.astraea.app.consumer;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.astraea.app.performance.MutableMetric;

public abstract class Builder<Key, Value> {
  protected final Map<String, Object> configs = new HashMap<>();
  protected Deserializer<?> keyDeserializer = Deserializer.BYTE_ARRAY;
  protected Deserializer<?> valueDeserializer = Deserializer.BYTE_ARRAY;

  protected SeekStrategy seekStrategy = SeekStrategy.NONE;
  protected long seekValue = -1;

  Builder() {}

  /**
   * make the consumer read data from beginning. By default, it reads the latest data.
   *
   * @return this builder
   */
  public Builder<Key, Value> fromBeginning() {
    this.configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return this;
  }

  /**
   * make the consumer read data from latest. this is default setting.
   *
   * @return this builder
   */
  public Builder<Key, Value> fromLatest() {
    this.configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    return this;
  }

  /**
   * Set seek strategy name and its accepted value, seek strategies are as follows:
   * <li>{@link SeekStrategy#DISTANCE_FROM_LATEST DISTANCE_FROM_LATEST}: set the offset to read from
   *     the latest offset. For example, the end offset is 5, and you set {@code value} to 2,then
   *     you will read data from offset: 3
   * <li>{@link SeekStrategy#DISTANCE_FROM_BEGINNING DISTANCE_FROM_BEGINNING}: set the offset to
   *     read from the beginning offset. For example, the beginning offset is 0, and you set {@code
   *     value} to 2, then you will read data from offset: 2
   * <li>{@link SeekStrategy#SEEK_TO SEEK_TO}: set the offset to read from. For example, the {@code
   *     value} is 15, then you will read data from offset: 15
   *
   * @param seekStrategy seek strategy
   * @param value the value that will send to seek strategy, if value < 0, throw {@code
   *     IllegalArgumentException}
   * @return this builder
   */
  public Builder<Key, Value> seekStrategy(SeekStrategy seekStrategy, long value) {
    this.seekStrategy = requireNonNull(seekStrategy);
    if (value < 0) {
      throw new IllegalArgumentException("seek value should >= 0");
    }
    this.seekValue = value;
    return this;
  }

  @SuppressWarnings("unchecked")
  public <NewKey> Builder<NewKey, Value> keyDeserializer(Deserializer<NewKey> keyDeserializer) {
    this.keyDeserializer = requireNonNull(keyDeserializer);
    return (Builder<NewKey, Value>) this;
  }

  @SuppressWarnings("unchecked")
  public <NewValue> Builder<Key, NewValue> valueDeserializer(
      Deserializer<NewValue> valueDeserializer) {
    this.valueDeserializer = requireNonNull(valueDeserializer);
    return (Builder<Key, NewValue>) this;
  }

  public Builder<Key, Value> bootstrapServers(String bootstrapServers) {
    this.configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, requireNonNull(bootstrapServers));
    return this;
  }

  public Builder<Key, Value> isolation(Isolation isolation) {
    this.configs.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation.nameOfKafka());
    return this;
  }

  /** @return consumer instance. The different builders may return inherited consumer interface. */
  public abstract Consumer<Key, Value> build();

  protected static class BaseConsumer<Key, Value> implements Consumer<Key, Value> {
    protected final org.apache.kafka.clients.consumer.Consumer<Key, Value> kafkaConsumer;

    public BaseConsumer(org.apache.kafka.clients.consumer.Consumer<Key, Value> kafkaConsumer) {
      this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public Collection<Record<Key, Value>> poll(int recordCount, Duration timeout) {
      var end = System.currentTimeMillis() + timeout.toMillis();
      var records = new ArrayList<Record<Key, Value>>();
      while (records.size() < recordCount) {
        var remaining = end - System.currentTimeMillis();
        if (remaining <= 0) break;
        kafkaConsumer.poll(Duration.ofMillis(remaining)).forEach(r -> records.add(Record.of(r)));
      }
      return Collections.unmodifiableList(records);
    }

    @Override
    public MutableMetric<Double> getMetric(String metricName) {
      var kafkaMetric =
          kafkaConsumer.metrics().entrySet().stream()
              .filter(e -> e.getKey().name().equals(metricName))
              .findFirst()
              .orElseThrow()
              .getValue();
      return MutableMetric.of(kafkaMetric);
    }

    @Override
    public void wakeup() {
      kafkaConsumer.wakeup();
    }

    @Override
    public void close() {
      kafkaConsumer.close();
    }
  }

  public enum SeekStrategy {
    NONE((consumer, seekValue) -> {}),
    DISTANCE_FROM_LATEST(
        (kafkaConsumer, distanceFromLatest) -> {
          // this mode is not supported by kafka, so we have to calculate the offset first
          // 1) poll data until the assignment is completed
          while (kafkaConsumer.assignment().isEmpty()) {
            kafkaConsumer.poll(Duration.ofMillis(500));
          }
          var partitions = kafkaConsumer.assignment();
          // 2) get the end offsets from all subscribed partitions
          var endOffsets = kafkaConsumer.endOffsets(partitions);
          // 3) calculate and then seek to the correct offset (end offset - recent offset)
          endOffsets.forEach(
              (tp, latest) -> kafkaConsumer.seek(tp, Math.max(0, latest - distanceFromLatest)));
        }),
    DISTANCE_FROM_BEGINNING(
        (kafkaConsumer, distanceFromBeginning) -> {
          while (kafkaConsumer.assignment().isEmpty()) {
            kafkaConsumer.poll(Duration.ofMillis(500));
          }
          var partitions = kafkaConsumer.assignment();
          var beginningOffsets = kafkaConsumer.beginningOffsets(partitions);
          beginningOffsets.forEach(
              (tp, beginning) -> kafkaConsumer.seek(tp, beginning + distanceFromBeginning));
        }),
    SEEK_TO(
        (kafkaConsumer, seekTo) -> {
          while (kafkaConsumer.assignment().isEmpty()) {
            kafkaConsumer.poll(Duration.ofMillis(500));
          }
          var partitions = kafkaConsumer.assignment();
          partitions.forEach(tp -> kafkaConsumer.seek(tp, seekTo));
        });

    private final BiConsumer<org.apache.kafka.clients.consumer.Consumer<?, ?>, Long> function;

    SeekStrategy(BiConsumer<org.apache.kafka.clients.consumer.Consumer<?, ?>, Long> function) {
      this.function = requireNonNull(function);
    }

    void apply(org.apache.kafka.clients.consumer.Consumer<?, ?> kafkaConsumer, long seekValue) {
      function.accept(kafkaConsumer, seekValue);
    }
  }
}
