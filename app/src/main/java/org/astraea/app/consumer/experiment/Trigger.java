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
package org.astraea.app.consumer.experiment;

import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.common.Utils;

/** This class is responsible for trigger rebalance event. */
public class Trigger {
  private final AdminClient admin;
  private final ConsumerPool consumerPool;
  private final Random randomGenerator = new Random(System.currentTimeMillis());

  Trigger(ConsumerPool consumerPool, AdminClient admin) {
    this.consumerPool = consumerPool;
    this.admin = admin;
  }

  public void killAll() throws InterruptedException {
    consumerPool.killConsumers();
  }

  public void killConsumer() {
    if (consumerPool.range() > 1) consumerPool.killConsumer(victim());
  }

  private int victim() {
    return randomGenerator.nextInt(consumerPool.range());
  }

  public void addConsumer(Map<Integer, ConcurrentLinkedQueue<RebalanceTime>> generationIDTime) {
    consumerPool.addConsumer(generationIDTime);
  }

  public void addPartitionCount() {
    String topic = topic();
    Map<String, NewPartitions> addPartition =
        Map.of(topic, NewPartitions.increaseTo(partitions(Set.of(topic)) + 1));
    admin.createPartitions(addPartition);
    System.out.println("topic #" + topic + " increased partition");
  }

  private int partitions(Set<String> topics) {
    return Utils.packException(
        () ->
            admin.describeTopics(topics).all().get().entrySet().stream()
                .flatMap(
                    e ->
                        e.getValue().partitions().stream()
                            .map(p -> new TopicPartition(e.getKey(), p.partition())))
                .collect(Collectors.toSet())
                .size());
  }

  private String topic() {
    Set<String> topics;
    topics =
        Utils.packException(
            () -> admin.listTopics(new ListTopicsOptions().listInternal(false)).names().get());
    return topics.iterator().next();
  }

  public void enforce() {
    consumerPool.enforceRebalance(victim());
  }
}
