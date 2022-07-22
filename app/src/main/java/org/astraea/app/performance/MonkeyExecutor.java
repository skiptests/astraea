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

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.kafka.common.errors.WakeupException;
import org.astraea.app.concurrent.Executor;
import org.astraea.app.concurrent.State;

public class MonkeyExecutor implements Executor {

  private final Random random = new Random(System.currentTimeMillis());
  private final Performance.Argument argument;
  private final AtomicBoolean killed;
  private final AtomicBoolean restarted;
  private int livedConsumers;
  private final Supplier<Boolean> producerDone;
  private final Manager manager;

  MonkeyExecutor(
      Performance.Argument argument,
      AtomicBoolean killed,
      AtomicBoolean restarted,
      Supplier<Boolean> producerDone,
      Manager manager) {
    this.argument = argument;
    this.killed = killed;
    this.restarted = restarted;
    this.producerDone = producerDone;
    this.manager = manager;
    this.livedConsumers = argument.consumers;
  }

  @Override
  public State execute() throws InterruptedException {
    try {
      if (producerDone.get() && manager.consumedDone()) return State.DONE;
      int task = random.nextInt(2);
      TimeUnit.SECONDS.sleep((random.nextInt(argument.monkeyFreq) + 1));
      switch (Task.select(task)) {
        case KILL:
          if (livedConsumers > 0) {
            killed.set(true);
            livedConsumers -= 1;
            System.out.println("live consumer #" + livedConsumers);
            break;
          }
        case RESTART:
          if (livedConsumers != argument.consumers) {
            restarted.set(true);
            livedConsumers += 1;
            System.out.println("live consumer #" + livedConsumers);
            break;
          }
      }
      return State.RUNNING;
    } catch (WakeupException ignore) {
      return State.DONE;
    }
  }

  private enum Task {
    KILL,
    RESTART;

    static Task select(int taskNumber) {
      if (taskNumber == 0) {
        return KILL;
      }
      return RESTART;
    }
  }
}
