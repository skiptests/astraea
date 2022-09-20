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
import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.consumer.Isolation;
import org.astraea.common.producer.Acks;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PerformanceTest extends RequireBrokerCluster {

  @Test
  void testTransactionalProducer() {
    var topic = "testTransactionalProducer";
    String[] arguments1 = {
      "--bootstrap.servers", bootstrapServers(), "--topics", topic, "--transaction.size", "2"
    };
    var argument = Performance.Argument.parse(arguments1);
    try (var producer = argument.createProducer()) {
      Assertions.assertTrue(producer.transactional());
    }
  }

  @Test
  void testProducerExecutor() throws InterruptedException {
    var topic = "testProducerExecutor";
    String[] arguments1 = {
      "--bootstrap.servers", bootstrapServers(), "--topics", topic, "--compression", "gzip"
    };
    var latch = new CountDownLatch(1);
    var argument = Performance.Argument.parse(arguments1);
    try (var producer = argument.createProducer()) {
      Assertions.assertFalse(producer.transactional());
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
  void testNoTopic() {
    Assertions.assertThrows(
        ParameterException.class,
        () -> Performance.Argument.parse(new String[] {"--bootstrap.servers", bootstrapServers()}));
  }

  @Test
  void testCheckTopic() {
    var topic = Utils.randomString(10);
    var args =
        Performance.Argument.parse(
            new String[] {"--bootstrap.servers", bootstrapServers(), "--topics", topic});
    Assertions.assertThrows(IllegalArgumentException.class, args::checkTopics);

    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).create();
    }

    Utils.sleep(Duration.ofSeconds(2));
    args.checkTopics();
  }

  @Test
  void testPartialNonexistentTopic() {
    var existentTopic = initTopic();
    var arg =
        Performance.Argument.parse(
            new String[] {
              "--bootstrap.servers",
              bootstrapServers(),
              "--topics",
              Utils.randomString() + "," + existentTopic
            });
    Assertions.assertThrows(IllegalArgumentException.class, arg::checkTopics);
  }

  @Test
  void testChaosFrequency() {
    var args =
        Performance.Argument.parse(
            new String[] {
              "--bootstrap.servers",
              "localhost:9092",
              "--chaos.frequency",
              "10s",
              "--topics",
              initTopic()
            });
    Assertions.assertEquals(Duration.ofSeconds(10), args.chaosDuration);
  }

  @Test
  void testPartitionSupplier() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(6).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(2));
      var args =
          Performance.Argument.parse(
              new String[] {
                "--bootstrap.servers",
                bootstrapServers(),
                "--topics",
                topicName,
                "--specify.brokers",
                "1"
              });
      var expectedLeaders =
          admin.replicas(Set.of(topicName)).values().stream()
              .flatMap(Collection::stream)
              .filter(Replica::isLeader)
              .filter(r -> r.nodeInfo().id() == 1)
              .map(ReplicaInfo::topicPartition)
              .collect(Collectors.toUnmodifiableSet());

      // assert there are 3 brokers, the 6 partitions are divided
      Assertions.assertEquals(3, brokerIds().size());
      Assertions.assertEquals(2, expectedLeaders.size());

      var selector = args.topicPartitionSelector();
      var actual =
          IntStream.range(0, 1000)
              .mapToObj(ignored -> selector.get())
              .collect(Collectors.toUnmodifiableSet());

      Assertions.assertEquals(expectedLeaders, actual);

      // test multiple topics
      var topicName2 = Utils.randomString(10);
      admin.creator().topic(topicName2).numberOfPartitions(3).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(2));
      args =
          Performance.Argument.parse(
              new String[] {
                "--bootstrap.servers",
                bootstrapServers(),
                "--topics",
                topicName + "," + topicName2,
                "--specify.brokers",
                "1"
              });

      var expected2 =
          admin.replicas(Set.of(topicName, topicName2)).values().stream()
              .flatMap(Collection::stream)
              .filter(ReplicaInfo::isLeader)
              .filter(replica -> replica.nodeInfo().id() == 1)
              .map(ReplicaInfo::topicPartition)
              .collect(Collectors.toSet());
      var selector2 = args.topicPartitionSelector();
      var actual2 =
          IntStream.range(0, 10000)
              .mapToObj(ignored -> selector2.get())
              .collect(Collectors.toUnmodifiableSet());
      Assertions.assertEquals(expected2, actual2);

      // no specify broker
      Assertions.assertEquals(
          -1,
          Performance.Argument.parse(
                  new String[] {"--bootstrap.servers", bootstrapServers(), "--topics", topicName})
              .topicPartitionSelector()
              .get()
              .partition());

      // Test no partition in specified broker
      var topicName3 = Utils.randomString(10);
      admin.creator().topic(topicName3).numberOfPartitions(1).create();
      Utils.sleep(Duration.ofSeconds(2));
      var validBroker =
          admin.replicas(Set.of(topicName3)).values().stream()
              .findAny()
              .get()
              .get(0)
              .nodeInfo()
              .id();
      var noPartitionBroker = (validBroker == 3) ? 1 : validBroker + 1;
      args =
          Performance.Argument.parse(
              new String[] {
                "--bootstrap.servers",
                bootstrapServers(),
                "--topics",
                topicName3,
                "--specify.brokers",
                Integer.toString(noPartitionBroker)
              });
      Assertions.assertThrows(IllegalArgumentException.class, args::topicPartitionSelector);

      // test specify partitions
      var topicName4 = Utils.randomString();
      var topicName5 = Utils.randomString();
      admin.creator().topic(topicName4).numberOfPartitions(3).create();
      admin.creator().topic(topicName5).numberOfPartitions(3).create();
      Utils.sleep(Duration.ofSeconds(2));
      var targets =
          Set.of(
              TopicPartition.of(topicName4, 0),
              TopicPartition.of(topicName4, 1),
              TopicPartition.of(topicName5, 2));
      var arguments =
          Performance.Argument.parse(
              new String[] {
                "--bootstrap.servers",
                bootstrapServers(),
                "--specify.partitions",
                targets.stream().map(TopicPartition::toString).collect(Collectors.joining(",")),
                "--topics",
                initTopic()
              });
      var selector3 = arguments.topicPartitionSelector();

      Assertions.assertEquals(targets, Set.copyOf(arguments.specifyPartitions));
      Assertions.assertEquals(
          targets,
          IntStream.range(0, 10000)
              .mapToObj(ignore -> selector3.get())
              .collect(Collectors.toUnmodifiableSet()));

      // use specify.brokers in conjunction with specify.partitions will raise error
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              Performance.Argument.parse(
                      new String[] {
                        "--bootstrap.servers",
                        bootstrapServers(),
                        "--specify.partitions",
                        targets.stream()
                            .map(TopicPartition::toString)
                            .collect(Collectors.joining(",")),
                        "--specify.brokers",
                        "1,2",
                        "--topics",
                        initTopic()
                      })
                  .topicPartitionSelector());

      // use specify.partitions with nonexistent topic will raise error
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              Performance.Argument.parse(
                      new String[] {
                        "--bootstrap.servers",
                        bootstrapServers(),
                        "--topics",
                        initTopic(),
                        "--specify.partitions",
                        "NoSuchTopic-5566,Nonexistent-1024," + topicName4 + "-99999"
                      })
                  .topicPartitionSelector());

      // duplicate partitions in input doesn't affect the weight of each partition.
      final var duplicatedTp = TopicPartition.of(topicName4, 0);
      final var singleTp = TopicPartition.of(topicName4, 1);
      final var selector4 =
          Performance.Argument.parse(
                  new String[] {
                    "--bootstrap.servers",
                    bootstrapServers(),
                    "--topics",
                    initTopic(),
                    "--specify.partitions",
                    Stream.of(duplicatedTp, duplicatedTp, duplicatedTp, singleTp)
                        .map(TopicPartition::toString)
                        .collect(Collectors.joining(","))
                  })
              .topicPartitionSelector();
      var counting =
          IntStream.range(0, 10000)
              .mapToObj(ignore -> selector4.get())
              .collect(Collectors.groupingBy(x -> x, Collectors.counting()));

      var ratio = (double) (counting.get(duplicatedTp)) / counting.get(singleTp);
      Assertions.assertTrue(1.5 > ratio && ratio > 0.5);

      // --specify.partitions can't be use in conjunction with partitioner
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              Performance.Argument.parse(
                      new String[] {
                        "--bootstrap.servers",
                        bootstrapServers(),
                        "--topics",
                        initTopic(),
                        "--specify.partitions",
                        topicName4 + "-1",
                        "--partitioner",
                        RoundRobinPartitioner.class.getName()
                      })
                  .topicPartitionSelector());
    }
  }

  @Test
  void testLastOffsets() {
    var partitionCount = 40;
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      // large partitions
      admin.creator().topic(topicName).numberOfPartitions(partitionCount).create();
      Utils.sleep(Duration.ofSeconds(2));
      var args =
          Performance.Argument.parse(
              new String[] {"--bootstrap.servers", bootstrapServers(), "--topics", topicName});
      try (var producer = args.createProducer()) {
        IntStream.range(0, 250)
            .forEach(
                i -> producer.sender().topic(topicName).key(String.valueOf(i).getBytes()).run());
      }
      Assertions.assertEquals(partitionCount, args.lastOffsets().size());
      System.out.println(args.lastOffsets());
      args.lastOffsets().values().forEach(v -> Assertions.assertNotEquals(0, v));
    }
  }

  @Test
  void testAcks() {
    Stream.of(Acks.values())
        .forEach(
            ack -> {
              var arg =
                  Performance.Argument.parse(
                      new String[] {
                        "--bootstrap.servers",
                        bootstrapServers(),
                        "--acks",
                        ack.alias(),
                        "--topics",
                        initTopic()
                      });
              Assertions.assertEquals(ack, arg.acks);
            });
  }

  private static String initTopic() {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).create();
    }
    return topic;
  }

  @Test
  void testArgumentConflict() {
    var argument =
        new String[] {
          "--bootstrap.servers",
          "localhost:9092",
          "--topics",
          "ignore",
          "--interdependent.size",
          "3",
          "--partitioner",
          "org.astraea.common.partitioner.StrictCostDispatcher"
        };
    Assertions.assertDoesNotThrow(() -> Performance.Argument.parse(argument));

    var argument1 =
        new String[] {
          "--bootstrap.servers",
          "localhost:9092",
          "--topics",
          "ignore",
          "--interdependent.size",
          "3"
        };
    Assertions.assertThrows(ParameterException.class, () -> Performance.Argument.parse(argument1));
    var argument2 =
        new String[] {
          "--bootstrap.servers",
          "localhost:9092",
          "--topics",
          "ignore",
          "--interdependent.size",
          "3",
          "--partitioner",
          "org.apache.kafka.clients.producer.internals.DefaultPartitioner"
        };
    Assertions.assertThrows(ParameterException.class, () -> Performance.Argument.parse(argument2));
  }
}
