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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/** Used to record statistics. This is thread safe. */
public class Metrics implements BiConsumer<Long, Integer> {
  private double avgLatency;
  private long num;
  private long max;
  private long min;
  private long bytes;
  private long currentBytes;
  private final Map<MutableMetric<Double>, Long> realBytes = new HashMap<>();

  public Metrics() {
    avgLatency = 0;
    num = 0;
    max = 0;
    min = Long.MAX_VALUE;
    bytes = 0;
    currentBytes = 0;
  }

  /** Simultaneously add latency and bytes. */
  @Override
  public synchronized void accept(Long latency, Integer bytes) {
    ++num;
    putLatency(latency);
    addBytes(bytes);
  }

  /** Add a new value to latency metric. */
  private synchronized void putLatency(long latency) {
    min = Math.min(min, latency);
    max = Math.max(max, latency);
    avgLatency += (((double) latency) - avgLatency) / (double) num;
  }
  /** Add a new value to bytes. */
  private synchronized void addBytes(long bytes) {
    this.currentBytes += bytes;
    this.bytes += bytes;
  }

  /** @return Get the number of latency put. */
  public synchronized long num() {
    return num;
  }
  /** @return Get the maximum of latency put. */
  public synchronized long max() {
    return max;
  }
  /** @return Get the minimum of latency put. */
  public synchronized long min() {
    return min;
  }
  /** @return Get the average latency. */
  public synchronized double avgLatency() {
    return avgLatency;
  }

  /** @return total send/received bytes */
  public synchronized long bytes() {
    return bytes;
  }

  public synchronized long clearAndGetCurrentBytes() {
    var ans = currentBytes;
    currentBytes = 0;
    return ans;
  }

  public void putRealBytesMetric(MutableMetric<Double> realBytesMetric) {
    this.realBytes.put(realBytesMetric, 0L);
  }

  /**
   * Get current real bytes since last call. Warning: Real bytes is recorded in `realBytes`. Please
   * #setRealBytesMetric before get #currentRealBytes
   */
  public synchronized long currentRealBytes() {
    AtomicLong all = new AtomicLong(0L);

    // Get sum of all instances and update
    realBytes.replaceAll(
        (metric, last) -> {
          var totalRealBytes = (long) metric.metricValue().doubleValue();
          all.addAndGet(totalRealBytes - last);
          return totalRealBytes;
        });

    return all.get();
  }
}
