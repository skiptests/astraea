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
package org.astraea.app.admin;

import static org.junit.jupiter.api.condition.OS.WINDOWS;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.astraea.app.common.DataRate;
import org.astraea.app.common.Utils;
import org.astraea.app.concurrent.State;
import org.astraea.app.concurrent.ThreadPool;
import org.astraea.app.consumer.Consumer;
import org.astraea.app.consumer.Deserializer;
import org.astraea.app.producer.Producer;
import org.astraea.app.producer.Serializer;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class AdminTest extends RequireBrokerCluster {

  @Test
  void testCreator() {
    var topicName = "testCreator";
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topicName)
          .configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"))
          .create();
      Utils.waitFor(
          () ->
              admin
                  .topics()
                  .get(topicName)
                  .value(TopicConfig.COMPRESSION_TYPE_CONFIG)
                  .filter(value -> value.equals("lz4"))
                  .isPresent());

      var config = admin.topics().get(topicName);
      Assertions.assertEquals(
          config.keys().size(), (int) StreamSupport.stream(config.spliterator(), false).count());
      config.keys().forEach(key -> Assertions.assertTrue(config.value(key).isPresent()));
      Assertions.assertTrue(config.values().contains("lz4"));
    }
  }

  @Test
  void testCreateTopicRepeatedly() {
    var topicName = "testCreateTopicRepeatedly";
    try (var admin = Admin.of(bootstrapServers())) {
      Runnable createTopic =
          () ->
              admin
                  .creator()
                  .configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"))
                  .numberOfReplicas((short) 1)
                  .numberOfPartitions(3)
                  .topic(topicName)
                  .create();

      createTopic.run();
      Utils.waitFor(() -> admin.topics().containsKey(topicName));
      IntStream.range(0, 10).forEach(i -> createTopic.run());

      // changing number of partitions can producer error
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> admin.creator().numberOfPartitions(1).topic(topicName).create());

      // changing number of replicas can producer error
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> admin.creator().numberOfReplicas((short) 2).topic(topicName).create());

      // changing config can producer error
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              admin
                  .creator()
                  .configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip"))
                  .topic(topicName)
                  .create());
    }
  }

  @Test
  void testPartitions() {
    var topicName = "testPartitions";
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(3).create();
      // wait for syncing topic creation
      Utils.sleep(Duration.ofSeconds(5));
      Assertions.assertTrue(admin.topicNames().contains(topicName));
      var partitions = admin.replicas(Set.of(topicName));
      Assertions.assertEquals(3, partitions.size());
      var logFolders =
          logFolders().values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
      partitions
          .values()
          .forEach(
              replicas ->
                  replicas.forEach(
                      replica ->
                          Assertions.assertTrue(
                              logFolders.stream().anyMatch(replica.path()::contains))));
    }
  }

  @Test
  void testOffsets() {
    var topicName = "testOffsets";
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(3).create();
      // wait for syncing topic creation
      Utils.sleep(Duration.ofSeconds(3));
      var offsets = admin.offsets(Set.of(topicName));
      Assertions.assertEquals(3, offsets.size());
      offsets
          .values()
          .forEach(
              offset -> {
                Assertions.assertEquals(0, offset.earliest());
                Assertions.assertEquals(0, offset.latest());
              });
    }
  }

  @Test
  void testConsumerGroups() {
    var topicName = "testConsumerGroups-Topic";
    var consumerGroup = "testConsumerGroups-Group";
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(3).create();
      try (var c1 =
          Consumer.forTopics(Set.of(topicName))
              .bootstrapServers(bootstrapServers())
              .groupId(consumerGroup)
              .build()) {
        // wait for syncing topic creation
        Utils.sleep(Duration.ofSeconds(5));
        var consumerGroupMap = admin.consumerGroups(Set.of(consumerGroup));
        Assertions.assertEquals(1, consumerGroupMap.size());
        Assertions.assertTrue(consumerGroupMap.containsKey(consumerGroup));
        Assertions.assertEquals(consumerGroup, consumerGroupMap.get(consumerGroup).groupId());

        try (var c2 =
            Consumer.forTopics(Set.of(topicName))
                .bootstrapServers(bootstrapServers())
                .groupId("abc")
                .build()) {
          var count =
              admin.consumerGroupIds().stream()
                  .mapToInt(t -> admin.consumerGroups(Set.of(t)).size())
                  .sum();
          Assertions.assertEquals(count, admin.consumerGroups().size());
          Assertions.assertEquals(1, admin.consumerGroups(Set.of("abc")).size());
        }
      }
    }
  }

  @Test
  // There is a problem when migrating the log folder under Windows because the migrated source
  // cannot be deleted, so disabled this test on Windows for now.
  @DisabledOnOs(WINDOWS)
  void testMigrateSinglePartition() {
    var topicName = "testMigrateSinglePartition";
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(1).create();
      // wait for syncing topic creation
      Utils.sleep(Duration.ofSeconds(5));
      var broker = admin.brokerIds().iterator().next();
      admin.migrator().partition(topicName, 0).moveTo(List.of(broker));
      Utils.waitFor(
          () -> {
            var replicas = admin.replicas(Set.of(topicName));
            var partitionReplicas = replicas.entrySet().iterator().next().getValue();
            return replicas.size() == 1
                && partitionReplicas.size() == 1
                && partitionReplicas.get(0).broker() == broker;
          });

      var currentBroker =
          admin.replicas(Set.of(topicName)).get(TopicPartition.of(topicName, 0)).get(0).broker();
      var allPath = admin.brokerFolders(Set.of(currentBroker));
      var otherPath =
          allPath.get(currentBroker).stream()
              .filter(
                  i ->
                      !i.contains(
                          admin
                              .replicas(Set.of(topicName))
                              .get(TopicPartition.of(topicName, 0))
                              .get(0)
                              .path()))
              .collect(Collectors.toSet());
      admin
          .migrator()
          .partition(topicName, 0)
          .moveTo(Map.of(currentBroker, otherPath.iterator().next()));
      Utils.waitFor(
          () -> {
            var replicas = admin.replicas(Set.of(topicName));
            var partitionReplicas = replicas.entrySet().iterator().next().getValue();
            return replicas.size() == 1
                && partitionReplicas.size() == 1
                && partitionReplicas.get(0).path().equals(otherPath.iterator().next());
          });
    }
  }

  @Test
  void testDeclarePreferredLogDirectory() {
    // arrange
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = "DeclarePreferredLogDirectory_" + Utils.randomString();
      var topicPartition = TopicPartition.of(topic, 0);
      admin.creator().topic(topic).numberOfPartitions(1).numberOfReplicas((short) 1).create();
      Utils.sleep(Duration.ofSeconds(1));
      var originalBroker = admin.replicas(Set.of(topic)).get(topicPartition).get(0).broker();
      var nextBroker = (originalBroker + 1) % brokerIds().size();
      var nextDir = logFolders().get(nextBroker).stream().findAny().orElseThrow();
      Supplier<Replica> replicaNow = () -> admin.replicas(Set.of(topic)).get(topicPartition).get(0);

      // act, declare the preferred data directory
      Assertions.assertDoesNotThrow(
          () ->
              admin.migrator().partition(topic, 0).declarePreferredDir(Map.of(nextBroker, nextDir)),
          "The Migrator API should ignore the error");
      Utils.sleep(Duration.ofSeconds(1));

      // assert, nothing happened until the actual movement
      Assertions.assertNotEquals(nextBroker, replicaNow.get().broker());
      Assertions.assertNotEquals(nextDir, replicaNow.get().path());

      // act, perform the actual movement
      admin.migrator().partition(topic, 0).moveTo(List.of(nextBroker));
      Utils.sleep(Duration.ofSeconds(1));

      // assert, everything on the exact broker & dir
      Assertions.assertEquals(nextBroker, replicaNow.get().broker());
      Assertions.assertEquals(nextDir, replicaNow.get().path());
    }
  }

  @Test
  void testIllegalMigrationArgument() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = Utils.randomString();
      var topicPartition = TopicPartition.of(topic, 0);
      admin.creator().topic(topic).numberOfPartitions(1).numberOfReplicas((short) 1).create();
      Utils.sleep(Duration.ofSeconds(1));
      var currentReplica = admin.replicas(Set.of(topic)).get(topicPartition).get(0);
      var currentBroker = currentReplica.broker();
      var notExistReplica = (currentBroker + 1) % brokerIds().size();
      var nextDir = logFolders().get(notExistReplica).iterator().next();

      Assertions.assertThrows(
          IllegalStateException.class,
          () -> admin.migrator().partition(topic, 0).moveTo(Map.of(notExistReplica, nextDir)));
    }
  }

  @Test
  void testIllegalPreferredDirArgument() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = Utils.randomString();
      var topicPartition = TopicPartition.of(topic, 0);
      admin.creator().topic(topic).numberOfPartitions(1).numberOfReplicas((short) 1).create();
      Utils.sleep(Duration.ofSeconds(1));
      var currentReplica = admin.replicas(Set.of(topic)).get(topicPartition).get(0);
      var currentBroker = currentReplica.broker();
      var nextDir = logFolders().get(currentBroker).iterator().next();

      Assertions.assertThrows(
          IllegalStateException.class,
          () ->
              admin
                  .migrator()
                  .partition(topic, 0)
                  .declarePreferredDir(Map.of(currentBroker, nextDir)));
    }
  }

  @Test
  void testPartialMoveToArgument() {
    // arrange
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = Utils.randomString();
      admin.creator().topic(topic).numberOfPartitions(1).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(1));
      var replica0 = 0;
      var replica1 = 1;
      var replica2 = 2;
      var folder0 = logFolders().get(replica0).iterator().next();
      var folder1 = logFolders().get(replica1).iterator().next();
      var folder2 = logFolders().get(replica2).iterator().next();

      // act, assert
      Assertions.assertDoesNotThrow(
          () -> admin.migrator().partition(topic, 0).moveTo(Map.of(replica0, folder0)));
      Assertions.assertDoesNotThrow(
          () ->
              admin
                  .migrator()
                  .partition(topic, 0)
                  .moveTo(Map.of(replica0, folder0, replica1, folder1)));
      Assertions.assertDoesNotThrow(
          () ->
              admin
                  .migrator()
                  .partition(topic, 0)
                  .moveTo(Map.of(replica0, folder0, replica1, folder1, replica2, folder2)));
    }
  }

  @Test
  @DisabledOnOs(WINDOWS)
  void testMigrateAllPartitions() {
    var topicName = "testMigrateAllPartitions";
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(3).create();
      // wait for syncing topic creation
      Utils.sleep(Duration.ofSeconds(5));
      var broker = admin.brokerIds().iterator().next();
      admin.migrator().topic(topicName).moveTo(List.of(broker));
      Utils.waitFor(
          () -> {
            var replicas = admin.replicas(Set.of(topicName));
            if (replicas.size() != 3) return false;
            if (!replicas.values().stream().allMatch(rs -> rs.size() == 1)) return false;
            return replicas.values().stream()
                .allMatch(rs -> rs.stream().allMatch(r -> r.broker() == broker));
          });
    }
  }

  @Test
  void testReplicaSize() throws ExecutionException, InterruptedException {
    var topicName = "testReplicaSize";
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.builder().bootstrapServers(bootstrapServers()).build()) {
      producer.sender().topic(topicName).key(new byte[100]).run().toCompletableFuture().get();
      var originSize =
          admin.replicas(Set.of(topicName)).entrySet().iterator().next().getValue().get(0).size();

      // add data again
      producer.sender().topic(topicName).key(new byte[100]).run().toCompletableFuture().get();

      var newSize =
          admin.replicas(Set.of(topicName)).entrySet().iterator().next().getValue().get(0).size();
      Assertions.assertTrue(newSize > originSize);
    }
  }

  @Test
  void testCompact() {
    var topicName = "testCompacted";
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).compactionMaxLag(Duration.ofSeconds(1)).create();

      var key = "key";
      var anotherKey = "anotherKey";
      var value = "value";
      try (var producer =
          Producer.builder()
              .keySerializer(Serializer.STRING)
              .valueSerializer(Serializer.STRING)
              .bootstrapServers(bootstrapServers())
              .build()) {
        IntStream.range(0, 10)
            .forEach(i -> producer.sender().key(key).value(value).topic(topicName).run());
        producer.flush();

        // sleep and produce more data to generate the new segment
        Utils.sleep(Duration.ofSeconds(2));
        IntStream.range(0, 10)
            .forEach(i -> producer.sender().key(anotherKey).value(value).topic(topicName).run());
        producer.flush();
      }

      // sleep for compact (the backoff of compact thread is reduced to 2 seconds.
      // see org.astraea.service.Services)
      Utils.sleep(Duration.ofSeconds(3));

      try (var consumer =
          Consumer.forTopics(Set.of(topicName))
              .keyDeserializer(Deserializer.STRING)
              .valueDeserializer(Deserializer.STRING)
              .fromBeginning()
              .bootstrapServers(bootstrapServers())
              .build()) {

        var records =
            IntStream.range(0, 5)
                .mapToObj(i -> consumer.poll(Duration.ofSeconds(1)))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        Assertions.assertEquals(
            1, records.stream().filter(record -> record.key().equals(key)).count());

        // those data are in active segment, so they can be compacted.
        Assertions.assertEquals(
            10, records.stream().filter(record -> record.key().equals(anotherKey)).count());
      }
    }
  }

  @Test
  void testBrokerConfigs() {
    try (var admin = Admin.of(bootstrapServers())) {
      var brokerConfigs = admin.brokers();
      Assertions.assertEquals(3, brokerConfigs.size());
      brokerConfigs.values().forEach(c -> Assertions.assertNotEquals(0, c.keys().size()));
      Assertions.assertEquals(1, admin.brokers(Set.of(brokerIds().iterator().next())).size());
    }
  }

  @Test
  void testBrokerFolders() {
    try (var admin = Admin.of(bootstrapServers())) {
      Assertions.assertEquals(brokerIds().size(), admin.brokerFolders().size());
      // list all
      logFolders()
          .forEach(
              (id, ds) -> Assertions.assertEquals(admin.brokerFolders().get(id).size(), ds.size()));

      // list folders one by one
      logFolders()
          .forEach(
              (id, ds) -> {
                Assertions.assertEquals(1, admin.brokerFolders(Set.of(id)).size());
                Assertions.assertEquals(admin.brokerFolders(Set.of(id)).get(id).size(), ds.size());
              });
    }
  }

  @Test
  void testReplicas() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic("abc").numberOfPartitions(2).create();
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(2, admin.replicas(Set.of("abc")).size());

      var count = admin.topicNames().stream().mapToInt(t -> admin.replicas(Set.of(t)).size()).sum();
      Assertions.assertEquals(count, admin.replicas().size());
    }
  }

  @Test
  void testReplicasPreferredLeaderFlag() {
    // arrange
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = "testReplicasPreferredLeaderFlag_" + Utils.randomString();
      var partitionCount = 10;
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(partitionCount)
          .numberOfReplicas((short) 3)
          .create();
      Utils.sleep(Duration.ofSeconds(3));
      var expectedPreferredLeader =
          IntStream.range(0, partitionCount)
              .mapToObj(p -> TopicPartition.of(topic, p))
              .collect(Collectors.toUnmodifiableMap(p -> p, p -> List.of(0)));
      var currentPreferredLeader =
          (Supplier<Map<TopicPartition, List<Integer>>>)
              () ->
                  admin.replicas(Set.of(topic)).entrySet().stream()
                      .collect(
                          Collectors.toUnmodifiableMap(
                              Map.Entry::getKey,
                              entry ->
                                  entry.getValue().stream()
                                      .filter(Replica::isPreferredLeader)
                                      .map(Replica::broker)
                                      .collect(Collectors.toUnmodifiableList())));

      // act, make 0 be the preferred leader of every partition
      IntStream.range(0, partitionCount)
          .forEach(p -> admin.migrator().partition(topic, p).moveTo(List.of(0, 1, 2)));
      Utils.sleep(Duration.ofSeconds(3));

      // assert
      Assertions.assertEquals(expectedPreferredLeader, currentPreferredLeader.get());
    }
  }

  @Test
  void testProducerStates() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    try (var producer = Producer.of(bootstrapServers());
        var admin = Admin.of(bootstrapServers())) {
      producer.sender().topic(topic).value(new byte[1]).run().toCompletableFuture().get();

      var states = admin.producerStates();
      Assertions.assertNotEquals(0, states.size());
      var producerState =
          states.entrySet().stream()
              .filter(tp -> tp.getKey().topic().equals(topic))
              .flatMap(e -> e.getValue().stream())
              .collect(Collectors.toUnmodifiableList());
      Assertions.assertEquals(1, producerState.size());
    }
  }

  @Test
  void testIpQuota() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin.quotaCreator().ip("192.168.11.11").connectionRate(10).create();
      Utils.sleep(Duration.ofSeconds(2));

      java.util.function.Consumer<List<Quota>> checker =
          (quotas) -> {
            Assertions.assertEquals(1, quotas.size());
            Assertions.assertEquals(
                Set.of(Quota.Target.IP),
                quotas.stream().map(Quota::target).collect(Collectors.toSet()));
            Assertions.assertEquals(
                Set.of(Quota.Limit.IP_CONNECTION_RATE),
                quotas.stream().map(Quota::limit).collect(Collectors.toSet()));
            Assertions.assertEquals("192.168.11.11", quotas.iterator().next().targetValue());
            Assertions.assertEquals(10, quotas.iterator().next().limitValue());
          };

      // only target
      checker.accept(
          admin.quotas(Quota.Target.IP).stream().collect(Collectors.toUnmodifiableList()));

      // only target and name
      checker.accept(
          admin.quotas(Quota.Target.IP, "192.168.11.11").stream()
              .collect(Collectors.toUnmodifiableList()));
    }
  }

  @Test
  void testMultipleIpQuota() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin.quotaCreator().ip("192.168.11.11").connectionRate(10).create();
      admin.quotaCreator().ip("192.168.11.11").connectionRate(12).create();
      admin.quotaCreator().ip("192.168.11.11").connectionRate(9).create();
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(1, admin.quotas(Quota.Target.IP, "192.168.11.11").size());
    }
  }

  @Test
  void testClientQuota() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .quotaCreator()
          .clientId("my-id")
          .produceRate(DataRate.Byte.of(10L).perSecond())
          .consumeRate(DataRate.Byte.of(100L).perSecond())
          .create();
      Utils.sleep(Duration.ofSeconds(2));

      java.util.function.Consumer<List<Quota>> checker =
          (quotas) -> {
            Assertions.assertEquals(2, quotas.size());
            Assertions.assertEquals(
                Set.of(Quota.Target.CLIENT_ID),
                quotas.stream().map(Quota::target).collect(Collectors.toSet()));
            Assertions.assertEquals(
                Set.of(Quota.Limit.PRODUCER_BYTE_RATE, Quota.Limit.CONSUMER_BYTE_RATE),
                quotas.stream().map(Quota::limit).collect(Collectors.toSet()));
            Assertions.assertEquals(
                10,
                quotas.stream()
                    .filter(q -> q.limit() == Quota.Limit.PRODUCER_BYTE_RATE)
                    .findFirst()
                    .get()
                    .limitValue());
            Assertions.assertEquals(
                "my-id",
                quotas.stream()
                    .filter(q -> q.limit() == Quota.Limit.PRODUCER_BYTE_RATE)
                    .findFirst()
                    .get()
                    .targetValue());
            Assertions.assertEquals(
                100,
                quotas.stream()
                    .filter(q -> q.limit() == Quota.Limit.CONSUMER_BYTE_RATE)
                    .findFirst()
                    .get()
                    .limitValue());
            Assertions.assertEquals(
                "my-id",
                quotas.stream()
                    .filter(q -> q.limit() == Quota.Limit.CONSUMER_BYTE_RATE)
                    .findFirst()
                    .get()
                    .targetValue());
          };

      // only target
      checker.accept(
          admin.quotas(Quota.Target.CLIENT_ID).stream().collect(Collectors.toUnmodifiableList()));

      // only target and name
      checker.accept(
          admin.quotas(Quota.Target.CLIENT_ID, "my-id").stream()
              .collect(Collectors.toUnmodifiableList()));
    }
  }

  @Test
  void testMultipleClientQuota() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .quotaCreator()
          .clientId("my-id")
          .consumeRate(DataRate.Byte.of(100L).perSecond())
          .create();
      admin
          .quotaCreator()
          .clientId("my-id")
          .produceRate(DataRate.Byte.of(1000L).perSecond())
          .create();
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(2, admin.quotas(Quota.Target.CLIENT_ID, "my-id").size());
    }
  }

  @Test
  void testNodes() {
    try (var admin = Admin.of(bootstrapServers())) {
      final Set<NodeInfo> nodes = admin.nodes();
      Assertions.assertEquals(
          brokerIds(), nodes.stream().map(NodeInfo::id).collect(Collectors.toUnmodifiableSet()));
    }
  }

  @Test
  void testClusterInfo() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      String topic0 = "testClusterInfoFromAdmin_" + Utils.randomString(8);
      String topic1 = "testClusterInfoFromAdmin_" + Utils.randomString(8);
      String topic2 = "testClusterInfoFromAdmin_" + Utils.randomString(8);
      int partitionCount = 10;
      short replicaCount = 2;

      Stream.of(topic0, topic1, topic2)
          .forEach(
              topicName ->
                  admin
                      .creator()
                      .topic(topicName)
                      .numberOfPartitions(partitionCount)
                      .numberOfReplicas(replicaCount)
                      .create());
      Utils.sleep(Duration.ofSeconds(2));

      final var clusterInfo = admin.clusterInfo(Set.of(topic0, topic1, topic2));

      // ClusterInfo#nodes
      Assertions.assertEquals(
          brokerIds(),
          clusterInfo.nodes().stream().map(NodeInfo::id).collect(Collectors.toUnmodifiableSet()));
      // ClusterInfo#topics
      Assertions.assertEquals(Set.of(topic0, topic1, topic2), clusterInfo.topics());
      // ClusterInfo#replicas
      Assertions.assertEquals(partitionCount * replicaCount, clusterInfo.replicas(topic0).size());
      Assertions.assertEquals(partitionCount * replicaCount, clusterInfo.replicas(topic1).size());
      Assertions.assertEquals(partitionCount * replicaCount, clusterInfo.replicas(topic2).size());
      // ClusterInfo#dataDirectories
      brokerIds()
          .forEach(
              id -> Assertions.assertEquals(logFolders().get(id), clusterInfo.dataDirectories(id)));
      // ClusterInfo#availableReplicas
      Assertions.assertEquals(
          partitionCount * replicaCount, clusterInfo.availableReplicas(topic0).size());
      Assertions.assertEquals(
          partitionCount * replicaCount, clusterInfo.availableReplicas(topic1).size());
      Assertions.assertEquals(
          partitionCount * replicaCount, clusterInfo.availableReplicas(topic2).size());
      // ClusterInfo#availableReplicaLeaders
      Assertions.assertEquals(partitionCount, clusterInfo.availableReplicaLeaders(topic0).size());
      Assertions.assertEquals(partitionCount, clusterInfo.availableReplicaLeaders(topic1).size());
      Assertions.assertEquals(partitionCount, clusterInfo.availableReplicaLeaders(topic2).size());
      // No resource match found will raise exception
      Assertions.assertThrows(
          NoSuchElementException.class, () -> clusterInfo.replicas("Unknown Topic"));
      Assertions.assertThrows(
          NoSuchElementException.class, () -> clusterInfo.availableReplicas("Unknown Topic"));
      Assertions.assertThrows(
          NoSuchElementException.class, () -> clusterInfo.availableReplicaLeaders("Unknown Topic"));
      Assertions.assertThrows(NoSuchElementException.class, () -> clusterInfo.dataDirectories(-1));
      Assertions.assertThrows(NoSuchElementException.class, () -> clusterInfo.node(-1));
      Assertions.assertThrows(
          NoSuchElementException.class, () -> clusterInfo.node("unknown", 1024));
    }
  }

  @ParameterizedTest
  @ValueSource(shorts = {1, 2, 3})
  void preferredLeaderElection(short replicaSize) {
    var clusterSize = brokerIds().size();
    var topic = "preferredLeaderElection_" + Utils.randomString(6);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(30).numberOfReplicas(replicaSize).create();
      Utils.sleep(Duration.ofSeconds(3));

      var topicPartitions =
          IntStream.range(0, 30)
              .mapToObj(i -> TopicPartition.of(topic, i))
              .collect(Collectors.toUnmodifiableSet());

      var currentLeaderMap =
          (Supplier<Map<TopicPartition, Integer>>)
              () ->
                  admin.replicas(Set.of(topic)).entrySet().stream()
                      .collect(
                          Utils.toSortedMap(
                              Map.Entry::getKey,
                              e ->
                                  e.getValue().stream()
                                      .filter(Replica::leader)
                                      .findFirst()
                                      .orElseThrow()
                                      .broker()));
      var expectedReplicaList =
          currentLeaderMap.get().entrySet().stream()
              .collect(
                  Utils.toSortedMap(
                      Map.Entry::getKey,
                      entry -> {
                        int leaderBroker = entry.getValue();
                        return List.of(
                                (leaderBroker + 2) % clusterSize,
                                leaderBroker, // original leader
                                (leaderBroker + 1) % clusterSize)
                            .subList(0, replicaSize);
                      }));
      var expectedLeaderMap =
          (Supplier<Map<TopicPartition, Integer>>)
              () ->
                  expectedReplicaList.entrySet().stream()
                      .collect(
                          Utils.toSortedMap(
                              Map.Entry::getKey,
                              e -> e.getValue().stream().findFirst().orElseThrow()));

      // change replica list
      topicPartitions.forEach(
          topicPartition ->
              admin
                  .migrator()
                  .partition(topicPartition.topic(), topicPartition.partition())
                  .moveTo(expectedReplicaList.get(topicPartition)));
      Utils.sleep(Duration.ofSeconds(8));

      // ReplicaMigrator#moveTo will trigger leader election if current leader being kicked out of
      // replica list. This case is always true for replica size equals to 1.
      if (replicaSize == 1) {
        // ReplicaMigrator#moveTo will trigger leader election implicitly if the original leader is
        // kicked out of the replica list. Test if ReplicaMigrator#moveTo actually trigger leader
        // election implicitly.
        Assertions.assertEquals(expectedLeaderMap.get(), currentLeaderMap.get());

        // act, the Admin#preferredLeaderElection won't throw a ElectionNotNeededException
        topicPartitions.forEach(admin::preferredLeaderElection);
        Utils.sleep(Duration.ofSeconds(2));

        // after election
        Assertions.assertEquals(expectedLeaderMap.get(), currentLeaderMap.get());
      } else {
        // before election
        Assertions.assertNotEquals(expectedLeaderMap.get(), currentLeaderMap.get());

        // act
        topicPartitions.forEach(admin::preferredLeaderElection);
        Utils.sleep(Duration.ofSeconds(2));

        // after election
        Assertions.assertEquals(expectedLeaderMap.get(), currentLeaderMap.get());
      }
    }
  }

  @Test
  void testTransactionIds() throws ExecutionException, InterruptedException {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer =
            Producer.builder().bootstrapServers(bootstrapServers()).buildTransactional()) {
      Assertions.assertTrue(producer.transactional());
      producer.sender().key(new byte[10]).topic(topicName).run().toCompletableFuture().get();

      Assertions.assertTrue(admin.transactionIds().contains(producer.transactionId().get()));

      var transaction = admin.transactions().get(producer.transactionId().get());
      Assertions.assertNotNull(transaction);
      Assertions.assertEquals(
          transaction.state() == TransactionState.COMPLETE_COMMIT ? 0 : 1,
          transaction.topicPartitions().size());
    }
  }

  @Test
  void testTransactionIdsWithMultiPuts() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer =
            Producer.builder().bootstrapServers(bootstrapServers()).buildTransactional()) {
      Assertions.assertTrue(producer.transactional());
      IntStream.range(0, 10)
          .forEach(
              index ->
                  producer
                      .sender()
                      .key(String.valueOf(index).getBytes(StandardCharsets.UTF_8))
                      .topic(topicName)
                      .run());
      producer.flush();

      Assertions.assertTrue(admin.transactionIds().contains(producer.transactionId().get()));

      var transaction = admin.transactions().get(producer.transactionId().get());
      Assertions.assertNotNull(transaction);
      Assertions.assertEquals(
          transaction.state() == TransactionState.COMPLETE_COMMIT ? 0 : 1,
          transaction.topicPartitions().size());
    }
  }

  @Test
  void testRemoveAllMembers() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.builder().bootstrapServers(bootstrapServers()).build();
        var consumer =
            Consumer.forTopics(Set.of(topicName))
                .bootstrapServers(bootstrapServers())
                .fromBeginning()
                .build()) {
      producer.sender().topic(topicName).key(new byte[10]).run();
      producer.flush();
      Assertions.assertEquals(1, consumer.poll(1, Duration.ofSeconds(5)).size());

      producer.sender().topic(topicName).key(new byte[10]).run();
      producer.flush();
      admin.removeAllMembers(consumer.groupId());
      Assertions.assertEquals(
          0,
          admin
              .consumerGroups(Set.of(consumer.groupId()))
              .get(consumer.groupId())
              .activeMembers()
              .size());
    }
  }

  @Test
  void testRemoveStaticMembers() {
    var topicName = Utils.randomString(10);
    var staticId = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.builder().bootstrapServers(bootstrapServers()).build();
        var consumer =
            Consumer.forTopics(Set.of(topicName))
                .bootstrapServers(bootstrapServers())
                .groupInstanceId(staticId)
                .fromBeginning()
                .build()) {
      producer.sender().topic(topicName).key(new byte[10]).run();
      producer.flush();
      Assertions.assertEquals(1, consumer.poll(1, Duration.ofSeconds(5)).size());

      producer.sender().topic(topicName).key(new byte[10]).run();
      producer.flush();
      admin.removeStaticMembers(consumer.groupId(), Set.of(consumer.groupInstanceId().get()));
      Assertions.assertEquals(
          0,
          admin
              .consumerGroups(Set.of(consumer.groupId()))
              .get(consumer.groupId())
              .activeMembers()
              .size());
    }
  }

  @Test
  void testRemoveGroupWithDynamicMembers() {
    var groupId = Utils.randomString(10);
    var topicName = Utils.randomString(10);
    try (var consumer =
        Consumer.forTopics(Set.of(topicName))
            .bootstrapServers(bootstrapServers())
            .groupId(groupId)
            .build()) {
      Assertions.assertEquals(0, consumer.poll(Duration.ofSeconds(3)).size());
    }
    try (var admin = Admin.of(bootstrapServers())) {
      Assertions.assertTrue(admin.consumerGroupIds().contains(groupId));
      admin.removeGroup(groupId);
      Assertions.assertFalse(admin.consumerGroupIds().contains(groupId));
    }
  }

  @Test
  void testRemoveGroupWithStaticMembers() {
    var groupId = Utils.randomString(10);
    var topicName = Utils.randomString(10);
    try (var consumer =
        Consumer.forTopics(Set.of(topicName))
            .bootstrapServers(bootstrapServers())
            .groupId(groupId)
            .groupInstanceId(Utils.randomString(10))
            .build()) {
      Assertions.assertEquals(0, consumer.poll(Duration.ofSeconds(3)).size());
    }

    try (var admin = Admin.of(bootstrapServers())) {
      Assertions.assertTrue(admin.consumerGroupIds().contains(groupId));
      // the static member is existent
      Assertions.assertThrows(GroupNotEmptyException.class, () -> admin.removeGroup(groupId));
      // cleanup members
      admin.removeAllMembers(groupId);
      admin.removeGroup(groupId);
      Assertions.assertFalse(admin.consumerGroupIds().contains(groupId));
    }
  }

  @RepeatedTest(5) // this test is timer-based, so it needs to be looped for stability
  void testReassignmentWhenMovingPartitionToAnotherBroker() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(1).create();
      Utils.sleep(Duration.ofSeconds(3));

      var currentBroker =
          admin.replicas(Set.of(topicName)).get(TopicPartition.of(topicName, 0)).get(0).broker();
      var nextBroker = brokerIds().stream().filter(i -> i != currentBroker).findAny().get();

      try (var producer = Producer.of(bootstrapServers())) {
        var done = new AtomicBoolean(false);
        var data = new byte[1000];
        try (var pool =
            ThreadPool.builder()
                .executor(
                    () -> {
                      producer.sender().topic(topicName).key(data).value(data).run();
                      return done.get() ? State.DONE : State.RUNNING;
                    })
                .build()) {

          try {
            admin.migrator().topic(topicName).moveTo(List.of(nextBroker));
            var reassignment =
                admin.reassignments(Set.of(topicName)).get(TopicPartition.of(topicName, 0));

            // Don't verify the result if the migration is done
            if (reassignment != null) {
              Assertions.assertEquals(1, reassignment.from().size());
              var from = reassignment.from().iterator().next();
              Assertions.assertEquals(currentBroker, from.broker());
              Assertions.assertEquals(1, reassignment.to().size());
              var to = reassignment.to().iterator().next();
              Assertions.assertEquals(nextBroker, to.broker());
            }
          } finally {
            done.set(true);
          }
        }
      }
    }
  }

  @RepeatedTest(5) // this test is timer-based, so it needs to be looped for stability
  void testReassignmentWhenMovingPartitionToAnotherPath() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(1).create();
      Utils.sleep(Duration.ofSeconds(3));

      var currentReplica =
          admin.replicas(Set.of(topicName)).get(TopicPartition.of(topicName, 0)).get(0);
      var currentBroker = currentReplica.broker();
      var currentPath = currentReplica.path();
      var nextPath =
          logFolders().get(currentBroker).stream()
              .filter(p -> !p.equals(currentPath))
              .findFirst()
              .get();

      try (var producer = Producer.of(bootstrapServers())) {
        var done = new AtomicBoolean(false);
        var data = new byte[1000];
        try (var pool =
            ThreadPool.builder()
                .executor(
                    () -> {
                      producer.sender().topic(topicName).key(data).value(data).run();
                      return done.get() ? State.DONE : State.RUNNING;
                    })
                .build()) {

          try {
            admin.migrator().topic(topicName).moveTo(Map.of(currentReplica.broker(), nextPath));
            var reassignment =
                admin.reassignments(Set.of(topicName)).get(TopicPartition.of(topicName, 0));
            // Don't verify the result if the migration is done
            if (reassignment != null) {
              Assertions.assertEquals(1, reassignment.from().size());
              var from = reassignment.from().iterator().next();
              Assertions.assertEquals(currentBroker, from.broker());
              Assertions.assertEquals(currentPath, from.path());
              Assertions.assertEquals(1, reassignment.to().size());
              var to = reassignment.to().iterator().next();
              Assertions.assertEquals(currentBroker, to.broker());
              Assertions.assertEquals(nextPath, to.path());
            }
          } finally {
            done.set(true);
          }
        }
      }
    }
  }

  @RepeatedTest(5) // this test is timer-based, so it needs to be looped for stability
  void testMultiReassignments() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(1).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(3));

      var brokers = new ArrayList<>(brokerIds()).subList(0, 2);
      try (var producer = Producer.of(bootstrapServers())) {
        var done = new AtomicBoolean(false);
        var data = new byte[1000];
        try (var pool =
            ThreadPool.builder()
                .executor(
                    () -> {
                      producer.sender().topic(topicName).key(data).value(data).run();
                      return done.get() ? State.DONE : State.RUNNING;
                    })
                .build()) {

          try {
            admin.migrator().topic(topicName).moveTo(brokers);
            var reassignment =
                admin.reassignments(Set.of(topicName)).get(TopicPartition.of(topicName, 0));
            // Don't verify the result if the migration is done
            if (reassignment != null) {
              Assertions.assertEquals(3, reassignment.from().size());
              Assertions.assertEquals(2, reassignment.to().size());
            }
          } finally {
            done.set(true);
          }
        }
      }
    }
  }

  @Test
  void testReassignmentWithNothing() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(1).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(3));
      try (var producer = Producer.of(bootstrapServers())) {
        producer.sender().topic(topicName).value(new byte[100]).run();
        producer.flush();
      }
      Assertions.assertEquals(0, admin.reassignments(Set.of(topicName)).size());
    }
  }

  @Test
  void testDeleteRecord() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(3).numberOfReplicas((short) 3).create();
      var deleteRecords = admin.deleteRecords(Map.of(TopicPartition.of(topicName, 0), 0L));

      Assertions.assertEquals(1, deleteRecords.size());
      Assertions.assertEquals(0, deleteRecords.values().stream().findFirst().get().lowWatermark());

      try (var producer = Producer.of(bootstrapServers())) {
        var senders =
            Stream.of(0, 0, 0, 1, 1)
                .map(x -> producer.sender().topic(topicName).partition(x).value(new byte[100]))
                .collect(Collectors.toList());
        producer.send(senders);
        producer.flush();
      }

      deleteRecords =
          admin.deleteRecords(
              Map.of(TopicPartition.of(topicName, 0), 2L, TopicPartition.of(topicName, 1), 1L));
      Assertions.assertEquals(2, deleteRecords.size());
      Assertions.assertEquals(2, deleteRecords.get(TopicPartition.of(topicName, 0)).lowWatermark());
      Assertions.assertEquals(1, deleteRecords.get(TopicPartition.of(topicName, 1)).lowWatermark());

      var offsets = admin.offsets();
      Assertions.assertEquals(2, offsets.get(TopicPartition.of(topicName, 0)).earliest());
      Assertions.assertEquals(1, offsets.get(TopicPartition.of(topicName, 1)).earliest());
      Assertions.assertEquals(0, offsets.get(TopicPartition.of(topicName, 2)).earliest());
    }
  }
}
