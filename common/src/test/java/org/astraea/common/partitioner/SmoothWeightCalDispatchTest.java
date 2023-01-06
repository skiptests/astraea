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
package org.astraea.common.partitioner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.common.FutureUtils;
import org.astraea.common.Header;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.consumer.Deserializer;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.common.producer.Serializer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SmoothWeightCalDispatchTest extends RequireBrokerCluster {
  private final String brokerList = bootstrapServers();
  private final Admin admin = Admin.of(bootstrapServers());

  private Map<String, String> initProConfig() {
    return Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        brokerList,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class.getName(),
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class.getName(),
        ProducerConfig.CLIENT_ID_CONFIG,
        "id1",
        ProducerConfig.PARTITIONER_CLASS_CONFIG,
        SmoothWeightRoundRobinDispatcher.class.getName(),
        "producerID",
        "1",
        "broker.0.jmx.port",
        String.valueOf(jmxServiceURL().getPort()),
        "broker.1.jmx.port",
        String.valueOf(jmxServiceURL().getPort()),
        "broker.2.jmx.port",
        String.valueOf(jmxServiceURL().getPort()));
  }

  @Test
  void testPartitioner() {
    var topicName = "address";
    admin.creator().topic(topicName).numberOfPartitions(10).run().toCompletableFuture().join();
    var key = "tainan";
    var timestamp = System.currentTimeMillis() + 10;
    var header = Header.of("a", "b".getBytes());
    try (var producer =
        Producer.builder()
            .keySerializer(Serializer.STRING)
            .configs(
                initProConfig().entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())))
            .build()) {
      var i = 0;
      while (i < 300) {
        var metadata =
            producer
                .send(
                    Record.builder()
                        .topic(topicName)
                        .key(key)
                        .timestamp(timestamp)
                        .headers(List.of(header))
                        .build())
                .toCompletableFuture()
                .join();
        assertEquals(topicName, metadata.topic());
        assertEquals(timestamp, metadata.timestamp());
        i++;
      }
    }
    Utils.sleep(Duration.ofSeconds(1));
    try (var consumer =
        Consumer.forTopics(Set.of(topicName))
            .bootstrapServers(bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .keyDeserializer(Deserializer.STRING)
            .build()) {
      var records = consumer.poll(Duration.ofSeconds(20));
      var recordsCount = records.size();
      while (recordsCount < 300) {
        recordsCount += consumer.poll(Duration.ofSeconds(20)).size();
      }
      assertEquals(300, recordsCount);
      var record = records.iterator().next();
      assertEquals(topicName, record.topic());
      assertEquals("tainan", record.key());
      assertEquals(1, record.headers().size());
      var actualHeader = record.headers().iterator().next();
      assertEquals(header.key(), actualHeader.key());
      Assertions.assertArrayEquals(header.value(), actualHeader.value());
    }
  }

  @Test
  void testMultipleProducer() {
    var topicName = "addr";
    admin.creator().topic(topicName).numberOfPartitions(10).run().toCompletableFuture().join();
    var key = "tainan";
    var timestamp = System.currentTimeMillis() + 10;
    var header = Header.of("a", "b".getBytes());

    FutureUtils.sequence(
            IntStream.range(0, 10)
                .mapToObj(
                    ignored ->
                        CompletableFuture.runAsync(
                            producerThread(
                                Producer.builder()
                                    .keySerializer(Serializer.STRING)
                                    .configs(
                                        initProConfig().entrySet().stream()
                                            .collect(
                                                Collectors.toMap(
                                                    e -> e.getKey(), e -> e.getValue())))
                                    .build(),
                                topicName,
                                key,
                                header,
                                timestamp)))
                .collect(Collectors.toUnmodifiableList()))
        .join();

    try (var consumer =
        Consumer.forTopics(Set.of(topicName))
            .bootstrapServers(bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .keyDeserializer(Deserializer.STRING)
            .build()) {
      var records = consumer.poll(Duration.ofSeconds(20));
      var recordsCount = records.size();
      while (recordsCount < 1000) {
        recordsCount += consumer.poll(Duration.ofSeconds(20)).size();
      }
      assertEquals(1000, recordsCount);
      var record = records.iterator().next();
      assertEquals(topicName, record.topic());
      assertEquals("tainan", record.key());
      assertEquals(1, record.headers().size());
      var actualHeader = record.headers().iterator().next();
      assertEquals(header.key(), actualHeader.key());
      Assertions.assertArrayEquals(header.value(), actualHeader.value());
    }
  }

  @Test
  void testJmxConfig() throws IOException {
    var props = initProConfig();
    var file =
        new File(
            SmoothWeightCalDispatchTest.class.getResource("").getPath() + "PartitionerConfigTest");
    try (var fileWriter = new FileWriter(file)) {
      fileWriter.write(
          "broker.0.jmx.port="
              + jmxServiceURL().getPort()
              + "\n"
              + "broker.1.jmx.port="
              + jmxServiceURL().getPort()
              + "\n"
              + "broker.2.jmx.port="
              + jmxServiceURL().getPort()
              + "\n");
      fileWriter.flush();
    }
    var topicName = "addressN";
    admin.creator().topic(topicName).numberOfPartitions(10).run().toCompletableFuture().join();
    var key = "tainan";
    var timestamp = System.currentTimeMillis() + 10;
    var header = Header.of("a", "b".getBytes());

    try (var producer =
        Producer.builder()
            .keySerializer(Serializer.STRING)
            .configs(
                props.entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())))
            .build()) {
      var metadata =
          producer
              .send(
                  Record.builder()
                      .topic(topicName)
                      .key(key)
                      .timestamp(timestamp)
                      .headers(List.of(header))
                      .build())
              .toCompletableFuture()
              .join();
      assertEquals(topicName, metadata.topic());
      assertEquals(timestamp, metadata.timestamp());
    }
  }

  private Runnable producerThread(
      Producer<String, byte[]> producer, String topic, String key, Header header, long timeStamp) {
    return () -> {
      try (producer) {
        var i = 0;
        while (i <= 99) {
          producer.send(
              Record.builder()
                  .topic(topic)
                  .key(key)
                  .timestamp(timeStamp)
                  .headers(List.of(header))
                  .build());
          i++;
        }
        producer.flush();
      }
    };
  }
}