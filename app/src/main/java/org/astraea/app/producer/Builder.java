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
package org.astraea.app.producer;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.astraea.app.admin.Compression;
import org.astraea.app.performance.MutableMetric;

public class Builder<Key, Value> {
  private final Map<String, Object> configs = new HashMap<>();
  private Serializer<?> keySerializer = Serializer.BYTE_ARRAY;
  private Serializer<?> valueSerializer = Serializer.BYTE_ARRAY;

  Builder() {}

  @SuppressWarnings("unchecked")
  public <NewKey> Builder<NewKey, Value> keySerializer(Serializer<NewKey> keySerializer) {
    this.keySerializer = Objects.requireNonNull(keySerializer);
    return (Builder<NewKey, Value>) this;
  }

  @SuppressWarnings("unchecked")
  public <NewValue> Builder<Key, NewValue> valueSerializer(Serializer<NewValue> valueSerializer) {
    this.valueSerializer = Objects.requireNonNull(valueSerializer);
    return (Builder<Key, NewValue>) this;
  }

  public Builder<Key, Value> config(String key, String value) {
    this.configs.put(key, value);
    return this;
  }

  public Builder<Key, Value> configs(Map<String, String> configs) {
    this.configs.putAll(configs);
    return this;
  }

  public Builder<Key, Value> bootstrapServers(String bootstrapServers) {
    return config(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(bootstrapServers));
  }

  public Builder<Key, Value> partitionClassName(String partitionClassName) {
    return config(
        ProducerConfig.PARTITIONER_CLASS_CONFIG, Objects.requireNonNull(partitionClassName));
  }

  public Builder<Key, Value> compression(Compression compression) {
    return config(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression.nameOfKafka());
  }

  /**
   * set the transaction id. If you set the transaction id, the builder will always build
   * transactional producer.
   *
   * @param transactionId used to trace by server
   * @return this builder
   */
  public Builder<Key, Value> transactionId(String transactionId) {
    return config(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
  }

  private static <Key, Value> CompletionStage<Metadata> doSend(
      org.apache.kafka.clients.producer.Producer<Key, Value> producer,
      ProducerRecord<Key, Value> record) {
    var completableFuture = new CompletableFuture<Metadata>();
    producer.send(
        record,
        (metadata, exception) -> {
          if (exception == null) completableFuture.complete(Metadata.of(metadata));
          else completableFuture.completeExceptionally(exception);
        });
    return completableFuture;
  }

  /**
   * @return normal producer if there is no transaction id in configs. Otherwise, a transactional
   *     producer is returned
   */
  @SuppressWarnings("unchecked")
  public Producer<Key, Value> build() {
    // if user configs the transaction id, we should build transactional producer
    if (configs.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG)) return buildTransactional();
    return new NormalProducer<>(
        new KafkaProducer<>(
            configs,
            Serializer.of((Serializer<Key>) keySerializer),
            Serializer.of((Serializer<Value>) valueSerializer)));
  }

  /**
   * @return always returns a transactional producer. The transaction id is generated automatically
   *     if it is absent
   */
  @SuppressWarnings("unchecked")
  public Producer<Key, Value> buildTransactional() {
    var transactionConfigs = new HashMap<>(configs);
    var transactionId =
        (String)
            transactionConfigs.computeIfAbsent(
                ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                ignored -> "transaction-id-" + new Random().nextLong());
    // For transactional send
    var transactionProducer =
        new KafkaProducer<>(
            transactionConfigs,
            Serializer.of((Serializer<Key>) keySerializer),
            Serializer.of((Serializer<Value>) valueSerializer));
    transactionProducer.initTransactions();
    return new TransactionalProducer<>(transactionProducer, transactionId);
  }

  private abstract static class BaseProducer<Key, Value> implements Producer<Key, Value> {
    protected final org.apache.kafka.clients.producer.Producer<Key, Value> kafkaProducer;

    private BaseProducer(org.apache.kafka.clients.producer.Producer<Key, Value> kafkaProducer) {
      this.kafkaProducer = kafkaProducer;
    }

    @Override
    public MutableMetric<Double> getMetric(String metricName) {
      var kafkaMetric =
          kafkaProducer.metrics().entrySet().stream()
              .filter(e -> e.getKey().name().equals(metricName))
              .findFirst()
              .orElseThrow()
              .getValue();
      return MutableMetric.of(kafkaMetric);
    }

    @Override
    public void flush() {
      kafkaProducer.flush();
    }

    @Override
    public void close() {
      kafkaProducer.close();
    }
  }

  private static class NormalProducer<Key, Value> extends BaseProducer<Key, Value> {
    private NormalProducer(org.apache.kafka.clients.producer.Producer<Key, Value> kafkaProducer) {
      super(kafkaProducer);
    }

    @Override
    public Sender<Key, Value> sender() {
      return new AbstractSender<>() {
        @Override
        public CompletionStage<Metadata> run() {
          return doSend(kafkaProducer, record());
        }
      };
    }

    @Override
    public Collection<CompletionStage<Metadata>> send(Collection<Sender<Key, Value>> senders) {
      return senders.stream().map(Sender::run).collect(Collectors.toUnmodifiableList());
    }

    @Override
    public Optional<String> transactionId() {
      return Optional.empty();
    }
  }

  private static class TransactionalProducer<Key, Value> extends BaseProducer<Key, Value> {
    private final Object lock = new Object();
    private final String transactionId;

    private TransactionalProducer(
        org.apache.kafka.clients.producer.Producer<Key, Value> kafkaProducer,
        String transactionId) {
      super(kafkaProducer);
      this.transactionId = transactionId;
    }

    @Override
    public Sender<Key, Value> sender() {
      return new AbstractSender<>() {
        @Override
        public CompletionStage<Metadata> run() {
          return send(List.of(this)).iterator().next();
        }
      };
    }

    @Override
    public Collection<CompletionStage<Metadata>> send(Collection<Sender<Key, Value>> senders) {
      var invalidSenders =
          senders.stream()
              .filter(s -> !(s instanceof AbstractSender))
              .collect(Collectors.toUnmodifiableList());
      if (!invalidSenders.isEmpty())
        throw new IllegalArgumentException(
            "those senders: "
                + invalidSenders.stream()
                    .map(Sender::getClass)
                    .map(Class::getName)
                    .collect(Collectors.joining(","))
                + " are not supported");
      try {
        synchronized (lock) {
          kafkaProducer.beginTransaction();
          var futures =
              senders.stream()
                  .map(s -> (AbstractSender<Key, Value>) s)
                  .map(s -> doSend(kafkaProducer, s.record()))
                  .collect(Collectors.toUnmodifiableList());
          kafkaProducer.commitTransaction();
          return futures;
        }
      } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
        kafkaProducer.close();
        // Error occur
        throw e;
      } catch (KafkaException ke) {
        kafkaProducer.abortTransaction();
        return send(senders);
      }
    }

    @Override
    public boolean transactional() {
      return true;
    }

    @Override
    public Optional<String> transactionId() {
      return Optional.of(transactionId);
    }
  }
}
