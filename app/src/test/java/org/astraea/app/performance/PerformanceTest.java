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
package org.astraea.app.performance;

import com.beust.jcommander.ParameterException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.argument.Argument;
import org.astraea.app.common.Utils;
import org.astraea.app.concurrent.Executor;
import org.astraea.app.concurrent.State;
import org.astraea.app.consumer.Consumer;
import org.astraea.app.consumer.Isolation;
import org.astraea.app.producer.Producer;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PerformanceTest extends RequireBrokerCluster {

  @Test
  void testTransactionalProducer() {
    var topic = "testTransactionalProducer";
    String[] arguments1 = {
      "--bootstrap.servers", bootstrapServers(), "--topic", topic, "--transaction.size", "2"
    };
    var latch = new CountDownLatch(1);
    Metrics observer =
        new Metrics() {
          @Override
          public void accept(Long x, Integer y) {
            latch.countDown();
          }
        };
    var argument = Argument.parse(new Performance.Argument(), arguments1);
    var producerExecutors =
        Performance.producerExecutors(
            argument,
            List.of(observer),
            () ->
                DataSupplier.data(
                    "key".getBytes(StandardCharsets.UTF_8),
                    "value".getBytes(StandardCharsets.UTF_8)),
            () -> -1);
    Assertions.assertEquals(1, producerExecutors.size());
    Assertions.assertTrue(producerExecutors.get(0).transactional());
  }

  @Test
  void testProducerExecutor() throws InterruptedException {
    var topic = "testProducerExecutor";
    String[] arguments1 = {
      "--bootstrap.servers", bootstrapServers(), "--topic", topic, "--compression", "gzip"
    };
    var latch = new CountDownLatch(1);
    Metrics observer =
        new Metrics() {
          @Override
          public void accept(Long x, Integer y) {
            latch.countDown();
          }
        };
    var argument = Argument.parse(new Performance.Argument(), arguments1);
    var producerExecutors =
        Performance.producerExecutors(
            argument,
            List.of(observer),
            () ->
                DataSupplier.data(
                    "key".getBytes(StandardCharsets.UTF_8),
                    "value".getBytes(StandardCharsets.UTF_8)),
            () -> -1);
    Assertions.assertEquals(1, producerExecutors.size());
    Assertions.assertFalse(producerExecutors.get(0).transactional());

    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).create();
      // wait for topic creation
      TimeUnit.SECONDS.sleep(2);
      admin.offsets(Set.of(topic)).values().forEach(o -> Assertions.assertEquals(0, o.latest()));

      Assertions.assertEquals(State.RUNNING, producerExecutors.get(0).execute());
      latch.await();
      Assertions.assertEquals(
          1, admin.offsets(Set.of(topic)).get(new TopicPartition(topic, 0)).latest());
    }
  }

  @Test
  void testConsumerExecutor() throws InterruptedException, ExecutionException {
    Metrics metrics = new Metrics();
    var topicName = "testConsumerExecutor-" + System.currentTimeMillis();
    var param = new Performance.Argument();
    param.sizeDistributionType = DistributionType.FIXED;
    try (Executor executor =
        Performance.consumerExecutor(
            Consumer.forTopics(Set.of(topicName)).bootstrapServers(bootstrapServers()).build(),
            metrics,
            new Manager(param, List.of(), List.of()),
            () -> false)) {
      executor.execute();

      Assertions.assertEquals(0, metrics.num());
      Assertions.assertEquals(0, metrics.bytes());

      try (var producer = Producer.builder().bootstrapServers(bootstrapServers()).build()) {
        producer.sender().topic(topicName).value(new byte[1024]).run().toCompletableFuture().get();
      }
      executor.execute();

      Assertions.assertEquals(1, metrics.num());
      Assertions.assertNotEquals(1024, metrics.bytes());
    }
  }

  @Test
  void testRealThroughput() throws InterruptedException {
    var topic = "testProducerExecutor-" + System.currentTimeMillis();
    Map<String, String> prop = Map.of(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    Metrics producerMetrics = new Metrics();

    try (var producer =
            Producer.builder().bootstrapServers(bootstrapServers()).configs(prop).build();
        Executor executor =
            ProducerExecutor.of(
                topic,
                producer,
                producerMetrics,
                () -> -1,
                () -> DataSupplier.data(new byte[8], new byte[1000]))) {
      producerMetrics.putRealBytesMetric(producer.getMetric("outgoing-byte-total"));

      // Compressed size should be less than raw record size.
      executor.execute();
      Utils.waitFor(() -> producerMetrics.num() == 1);
      var currentBytes = producerMetrics.clearAndGetCurrentBytes();
      var realBytes = producerMetrics.currentRealBytes();
      Assertions.assertNotEquals(0, currentBytes);
      Assertions.assertNotEquals(0, realBytes);
      Assertions.assertTrue(currentBytes > realBytes);
      Assertions.assertEquals(0, producerMetrics.currentRealBytes());
      Assertions.assertEquals(0, producerMetrics.clearAndGetCurrentBytes());
    }

    // Check for transactional producer real outgoing bytes
    try (var transactional =
            Producer.builder()
                .bootstrapServers(bootstrapServers())
                .configs(prop)
                .buildTransactional();
        Executor executor =
            ProducerExecutor.of(
                topic,
                5,
                transactional,
                producerMetrics,
                () -> -1,
                () -> DataSupplier.data(new byte[8], new byte[1000]))) {
      producerMetrics.putRealBytesMetric(transactional.getMetric("outgoing-byte-total"));

      // Compressed size should be less than raw record size.
      executor.execute();
      Utils.waitFor(() -> producerMetrics.num() >= 1);
      var currentBytes = producerMetrics.clearAndGetCurrentBytes();
      var realBytes = producerMetrics.currentRealBytes();
      Assertions.assertNotEquals(0, currentBytes);
      Assertions.assertNotEquals(0, realBytes);
      Assertions.assertTrue(currentBytes > realBytes);
      Assertions.assertEquals(0, producerMetrics.currentRealBytes());
      Assertions.assertEquals(0, producerMetrics.clearAndGetCurrentBytes());
    }

    Metrics consumerMetrics = new Metrics();
    try (var consumer =
            Consumer.forTopics(Set.of(topic))
                .bootstrapServers(bootstrapServers())
                .fromBeginning()
                .build();
        var executor =
            Performance.consumerExecutor(
                consumer,
                consumerMetrics,
                new Manager(new Performance.Argument(), List.of(), List.of()),
                () -> false)) {

      // Compressed size should be less than raw record size.
      executor.execute();
      var currentBytes = consumerMetrics.clearAndGetCurrentBytes();
      var realBytes = consumerMetrics.currentRealBytes();
      Assertions.assertNotEquals(0, currentBytes);
      Assertions.assertNotEquals(0, realBytes);
      Assertions.assertTrue(currentBytes > realBytes);
      Assertions.assertEquals(0, consumerMetrics.currentRealBytes());
      Assertions.assertEquals(0, consumerMetrics.clearAndGetCurrentBytes());
    }
  }

  @Test
  void testTransactionSet() {
    var argument = new Performance.Argument();
    Assertions.assertEquals(Isolation.READ_UNCOMMITTED, argument.isolation());
    argument.transactionSize = 1;
    Assertions.assertEquals(Isolation.READ_UNCOMMITTED, argument.isolation());
    argument.transactionSize = 3;
    Assertions.assertEquals(Isolation.READ_COMMITTED, argument.isolation());
  }

  @Test
  void testArgument() {
    String[] arguments1 = {
      "--bootstrap.servers",
      "localhost:9092",
      "--topic",
      "not-empty",
      "--partitions",
      "10",
      "--replicas",
      "3",
      "--producers",
      "1",
      "--consumers",
      "1",
      "--run.until",
      "1000records",
      "--record.size",
      "10KiB",
      "--partitioner",
      "org.astraea.partitioner.smooth.SmoothWeightPartitioner",
      "--compression",
      "lz4",
      "--key.distribution",
      "zipfian",
      "--specify.broker",
      "1",
      "--configs",
      "key=value"
    };

    var arg = Argument.parse(new Performance.Argument(), arguments1);
    Assertions.assertEquals("value", arg.configs().get("key"));

    String[] arguments2 = {"--bootstrap.servers", "localhost:9092", "--topic", ""};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments2));

    String[] arguments3 = {"--bootstrap.servers", "localhost:9092", "--replicas", "0"};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments3));

    String[] arguments4 = {"--bootstrap.servers", "localhost:9092", "--partitions", "0"};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments4));

    String[] arguments5 = {"--bootstrap.servers", "localhost:9092", "--producers", "0"};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments5));

    String[] arguments6 = {"--bootstrap.servers", "localhost:9092", "--consumers", "0"};
    Assertions.assertDoesNotThrow(() -> Argument.parse(new Performance.Argument(), arguments6));

    String[] arguments7 = {"--bootstrap.servers", "localhost:9092", "--run.until", "1"};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments7));

    String[] arguments8 = {"--bootstrap.servers", "localhost:9092", "--record.size", "1"};
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Argument.parse(new Performance.Argument(), arguments8));

    String[] arguments10 = {"--bootstrap.servers", "localhost:9092", "--partitioner", ""};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments10));

    String[] arguments11 = {"--bootstrap.servers", "localhost:9092", "--compression", ""};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments11));

    String[] arguments12 = {"--bootstrap.servers", "localhost:9092", "--key.distribution", ""};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments12));

    String[] arguments13 = {"--bootstrap.servers", "localhost:9092", "--specify.broker", ""};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments13));
  }
}
