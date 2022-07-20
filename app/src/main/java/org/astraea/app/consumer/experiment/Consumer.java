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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.kafka.clients.consumer.KafkaConsumer;

class Consumer extends Thread {
  private final int id;
  private final KafkaConsumer<?, ?> consumer;
  private final Listener listener;
  private final Map<Integer, ConcurrentLinkedQueue<RebalanceTime>> timePerGeneration;
  private boolean isEnforce;

  Consumer(
      int id,
      KafkaConsumer<?, ?> consumer,
      Map<Integer, ConcurrentLinkedQueue<RebalanceTime>> timePerGeneration) {
    this.consumer = consumer;
    this.id = id;
    this.timePerGeneration = timePerGeneration;
    this.listener = new Listener(id, consumer, timePerGeneration);
  }

  public void doSubscribe(Set<String> topics) {
    consumer.subscribe(topics, listener);
  }

  public void enforce() {
    isEnforce = true;
  }

  public int id() {
    return id;
  }

  @Override
  public void run() {
    try {
      System.out.println("Start consumer #" + id);
      while (!Thread.currentThread().isInterrupted()) {
        consumer.poll(Duration.ofMillis(250));
        if (isEnforce) {
          consumer.enforceRebalance();
          isEnforce = false;
        }
      }
    } catch (Exception e) {
      System.out.println("Close consumer #" + id);
      Thread.interrupted();
    } finally {
      consumer.close();
    }
  }
}
