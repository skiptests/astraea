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

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.astraea.app.common.Utils;

/** This class manages consumer instance(s) */
public class ConsumerPool {
  private final ArrayList<Consumer> consumers;
  private final Main.Argument argument;
  private final AdminClient admin;
  private int id;

  ConsumerPool(ArrayList<Consumer> consumers, Main.Argument argument, AdminClient admin) {
    this.consumers = consumers;
    this.argument = argument;
    this.admin = admin;
    this.id = argument.consumers;
  }

  /**
   * Create consumer instance(s) and put them into ArrayList, finally start them. The number of
   * consumer instance was defined by user
   */
  public void initialPool(Map<Integer, ConcurrentLinkedQueue<RebalanceTime>> generationIDTime) {
    createConsumers(generationIDTime);
    subscribeAll(topicNames());
    consumers.forEach(Thread::start);
  }

  public int range() {
    return consumers.size();
  }

  private void createConsumers(
      Map<Integer, ConcurrentLinkedQueue<RebalanceTime>> generationIDTime) {
    IntStream.range(0, argument.consumers)
        .boxed()
        .forEach(i -> consumers.add(consumer(i, generationIDTime)));
  }

  private Consumer consumer(
      int id, Map<Integer, ConcurrentLinkedQueue<RebalanceTime>> generationIDTime) {
    return new Consumer(
        id,
        new KafkaConsumer<>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                argument.bootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                argument.keyDeserializer,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                argument.valueDeserializer,
                ConsumerConfig.GROUP_ID_CONFIG,
                argument.groupId,
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                argument.strategy)),
        generationIDTime);
  }

  private Set<String> topicNames() {
    return Utils.packException(
        () -> admin.listTopics(new ListTopicsOptions().listInternal(false)).names().get());
  }

  private void subscribeAll(Set<String> topics) {
    consumers.forEach(consumer -> consumer.doSubscribe(topics));
  }

  /** Kill a consumer randomly. */
  public void killConsumer(int victim) {
    consumers.get(victim).interrupt();
    consumers.remove(victim);
  }

  /** Kill all consumer in consumers one by one. */
  public void killConsumers() throws InterruptedException {
    for (Consumer consumer : consumers) {
      TimeUnit.SECONDS.sleep(10);
      consumer.interrupt();
    }
  }
  /** Add a consumer to the consumer pool and start it. */
  public void addConsumer(Map<Integer, ConcurrentLinkedQueue<RebalanceTime>> generationIDTime) {
    consumers.add(consumer(id, generationIDTime));
    Consumer consumer = consumers.get(consumers.size() - 1);
    consumer.doSubscribe(topicNames());
    consumer.start();
    id += 1;
  }

  public void enforceRebalance(int victim) {
    consumers.get(victim).enforce();
    System.out.println("consumer #" + consumers.get(0).id() + " enforce rebalance");
  }
}
