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

import com.beust.jcommander.Parameter;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Main {
  static Map<Integer, ConcurrentLinkedQueue<RebalanceTime>> generationIDTime =
      new ConcurrentSkipListMap<>();

  public static void main(String[] args) throws InterruptedException {
    Argument argument = org.astraea.app.argument.Argument.parse(new Argument(), args);
    AdminClient admin =
        AdminClient.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, argument.bootstrapServers()));
    ConsumerPool consumerPool = new ConsumerPool(new ArrayList<>(), argument, admin);
    Trigger trigger = new Trigger(consumerPool, admin);
    Random random = new Random(System.currentTimeMillis());
    int task = 0;

    consumerPool.initialPool(generationIDTime);

    long startTime = System.currentTimeMillis() / 1000;

    while (time(startTime) < argument.duration) {
      TimeUnit.SECONDS.sleep(10);
      task = random.nextInt(3);
      if (task == 0) trigger.killConsumer();
      else if (task == 1) trigger.addConsumer(generationIDTime);
      else if (task == 2) {
        trigger.addPartitionCount();
        trigger.enforce();
        TimeUnit.SECONDS.sleep(20);
      }
      System.out.println(time(startTime));
    }
    trigger.killAll();

    printAvgTime();
  }

  static class Argument extends org.astraea.app.argument.Argument {
    @Parameter(names = "--keyDeserializer")
    String keyDeserializer = StringDeserializer.class.getName();

    @Parameter(names = "--valueDeserializer")
    String valueDeserializer = StringDeserializer.class.getName();

    @Parameter(names = "--groupId", required = true)
    String groupId;

    @Parameter(names = "--strategy")
    String strategy = RangeAssignor.class.getName();

    @Parameter(names = "--consumers")
    int consumers = 1;

    @Parameter(names = "--duration")
    int duration = 600;
  }

  static void printAvgTime() {
    generationIDTime.forEach(
        (generationId, rebalanceTimes) -> {
          System.out.println("generationId #" + generationId);
          final int size = rebalanceTimes.size();

          double avgTime =
              (double)
                      rebalanceTimes.stream()
                          .mapToLong((rebalanceTime -> rebalanceTime.rebalanceTime().toMillis()))
                          .sum()
                  / size;
          System.out.printf("Average time : %.2fms\n", avgTime);
        });
  }

  static long time(long startTime) {
    return ((System.currentTimeMillis() / 1000) - startTime);
  }
}
