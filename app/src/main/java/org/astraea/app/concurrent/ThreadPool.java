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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class offers a simple way to manage the threads. All threads added to builder are not
 * executing right now. Instead, all threads are starting when the pool is built.
 */
public interface ThreadPool extends AutoCloseable {
  // nothing to run.
  ThreadPool EMPTY =
      new ThreadPool() {
        @Override
        public void close() {}

        @Override
        public void waitAll() {}

        @Override
        public boolean isClosed() {
          return true;
        }

        @Override
        public int size() {
          return 0;
        }
      };

  /** close all running threads. */
  @Override
  void close();

  /** wait all executors to be done. */
  void waitAll();

  boolean isClosed();

  /** @return the number of threads */
  int size();

  /** put the executor into the thread pool and execute it. */
  default void putAndExecute(Executor executor) {}

  /** stop the executor which you specify. */
  default void stop(Executor executor) {}

  static Builder builder() {
    return new Builder();
  }

  class Builder {
    private final List<Executor> executors = new ArrayList<>();
    private final Set<Executor> stopExecutors = new HashSet<>();

    private Builder() {}

    public Builder executor(Executor executor) {
      return executors(List.of(executor));
    }

    public Builder executors(Collection<? extends Executor> executors) {
      this.executors.addAll(Objects.requireNonNull(executors));
      return this;
    }

    public ThreadPool build() {
      if (executors.isEmpty()) return EMPTY;
      var closed = new AtomicBoolean(false);
      var tasks = new AtomicInteger(executors.size());
      var service = Executors.newFixedThreadPool(executors.size());
      executors.forEach(executor -> service.execute(() -> isDone(closed, tasks, executor)));
      return new ThreadPool() {
        @Override
        public void close() {
          service.shutdownNow();
          closed.set(true);
          executors.forEach(Executor::wakeup);
          waitAll();
        }

        @Override
        public void waitAll() {
          try {
            while (!service.awaitTermination(2000, TimeUnit.MILLISECONDS)) {
              if (tasks.get() == 0) break;
            }
          } catch (InterruptedException e) {
            // swallow
          }
        }

        @Override
        public boolean isClosed() {
          return closed.get();
        }

        @Override
        public int size() {
          return executors.size();
        }

        @Override
        public void putAndExecute(Executor executor) {
          tasks.getAndUpdate(
              taskNumber -> {
                service.execute(() -> isDone(closed, tasks, executor));
                return taskNumber + 1;
              });
        }

        @Override
        public void stop(Executor executor) {
          tasks.getAndUpdate(
              taskNumber -> {
                if (taskNumber <= 0) return taskNumber;
                else {
                  stopExecutors.add(executor);
                  return taskNumber - 1;
                }
              });
        }
      };
    }

    private void isDone(AtomicBoolean closed, AtomicInteger tasks, Executor executor) {
      try {
        while (!closed.get()) {
          if ((executor.execute() == State.DONE) || (stopExecutors.remove(executor))) break;
        }
      } catch (InterruptedException e) {
        // swallow
      } finally {
        try {
          executor.close();
        } finally {
          tasks.decrementAndGet();
        }
      }
    }
  }
}
