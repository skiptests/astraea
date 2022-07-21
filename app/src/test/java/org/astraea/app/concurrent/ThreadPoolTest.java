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
package org.astraea.app.concurrent;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.astraea.app.common.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class ThreadPoolTest {

  private static class CountExecutor implements Executor {

    private final AtomicInteger executeCount = new AtomicInteger();
    private final AtomicInteger closeCount = new AtomicInteger();
    private final AtomicInteger wakeupCount = new AtomicInteger();

    @Override
    public State execute() {
      executeCount.incrementAndGet();
      return State.RUNNING;
    }

    @Override
    public void close() {
      closeCount.incrementAndGet();
    }

    @Override
    public void wakeup() {
      wakeupCount.incrementAndGet();
    }
  }

  @Test
  void testSubmitThread() {
    var executor = new CountExecutor();
    try (var pool = ThreadPool.builder().executor(executor).build()) {
      Utils.sleep(Duration.ofSeconds(2));
    }
    Assertions.assertTrue(executor.executeCount.get() > 0);
    Assertions.assertEquals(1, executor.closeCount.get());
    Assertions.assertEquals(1, executor.wakeupCount.get());
  }

  @Test
  void testWaitAll() {
    try (var pool = ThreadPool.builder().executor(() -> State.DONE).build()) {
      pool.waitAll();
    }
  }

  @Timeout(10)
  @Test
  void testInterrupt() {
    var pool =
        ThreadPool.builder()
            .executor(
                () -> {
                  Utils.sleep(Duration.ofSeconds(100));
                  return State.DONE;
                })
            .build();
    pool.close();
    Assertions.assertTrue(pool.isClosed());
  }

  @Test
  void testEmpty() {
    try (var empty = ThreadPool.builder().build()) {
      Assertions.assertEquals(ThreadPool.EMPTY, empty);
      Assertions.assertEquals(0, empty.size());
      Assertions.assertTrue(empty.isClosed());
    }
  }

  @Test
  void testStop() {
    var stop1 = new AtomicBoolean(false);
    var stop2 = new AtomicBoolean(false);
    var stop3 = new AtomicBoolean(false);
    Executor e1 = createExecutor(stop1, new AtomicBoolean());
    Executor e2 = createExecutor(stop2, new AtomicBoolean());
    Executor e3 = createExecutor(stop3, new AtomicBoolean());
    try (var threadPool = ThreadPool.builder().executors(Set.of(e1, e2, e3)).build()) {
      Assertions.assertFalse(stop1.get());
      Assertions.assertFalse(stop2.get());
      Assertions.assertFalse(stop3.get());
      threadPool.stop(e1);
      Utils.sleep(Duration.ofMillis(10));
      Assertions.assertTrue(stop1.get());
      Assertions.assertFalse(stop2.get());
      Assertions.assertFalse(stop3.get());
    }
  }

  private Executor createExecutor(AtomicBoolean stop, AtomicBoolean execute) {
    return new Executor() {
      @Override
      public State execute() {
        try {
          execute.set(true);
          return State.RUNNING;
        } catch (Exception e) {
          // swallow
        }
        return State.DONE;
      }

      @Override
      public void close() {
        execute.set(false);
        stop.set(true);
      }
    };
  }

  @Test
  void testPutAndExecute() {
    var execute1 = new AtomicBoolean(false);
    var execute2 = new AtomicBoolean(false);
    var execute3 = new AtomicBoolean(false);
    Executor e1 = createExecutor(new AtomicBoolean(), execute1);
    Executor e2 = createExecutor(new AtomicBoolean(), execute2);
    Executor e3 = createExecutor(new AtomicBoolean(), execute3);
    try (var threadPool = ThreadPool.builder().executor(e1).executor(e2).build()) {
      Utils.sleep(Duration.ofMillis(10));
      Assertions.assertTrue(execute1.get());
      Assertions.assertTrue(execute2.get());
      Assertions.assertFalse(execute3.get());
      threadPool.putAndExecute(e3);
      Utils.sleep(Duration.ofMillis(10));
      Assertions.assertTrue(execute1.get());
      Assertions.assertTrue(execute2.get());
      Assertions.assertFalse(execute3.get());
      Utils.sleep(Duration.ofMillis(10));
      threadPool.stop(e1);
      Utils.sleep(Duration.ofMillis(10));
      Assertions.assertFalse(execute1.get());
      Assertions.assertTrue(execute2.get());
      Assertions.assertTrue(execute3.get());
    }
  }
}
