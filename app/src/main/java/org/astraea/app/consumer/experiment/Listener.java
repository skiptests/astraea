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

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

class Listener implements ConsumerRebalanceListener {

  private final int id;
  private long revokedTime = System.currentTimeMillis();
  private final KafkaConsumer<?, ?> consumer;
  private final Map<Integer, ConcurrentLinkedQueue<RebalanceTime>> generationIDTime;

  Listener(
      int id,
      KafkaConsumer<?, ?> consumer,
      Map<Integer, ConcurrentLinkedQueue<RebalanceTime>> generationIDTime) {
    this.consumer = consumer;
    this.id = id;
    this.generationIDTime = generationIDTime;
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    revokedTime = System.currentTimeMillis();
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    System.out.println("Assigned #" + id);
    long assignedTime = System.currentTimeMillis();
    Duration duration = Duration.ofMillis(assignedTime - revokedTime);
    RebalanceTime rebalanceTime = new RebalanceTime(duration, id);
    generationIDTime.putIfAbsent(
        consumer.groupMetadata().generationId(), new ConcurrentLinkedQueue<>());
    generationIDTime.get(consumer.groupMetadata().generationId()).add(rebalanceTime);
  }

  @Override
  public void onPartitionsLost(Collection<TopicPartition> partitions) {
    System.out.println("#" + id + " lost");
  }
}
