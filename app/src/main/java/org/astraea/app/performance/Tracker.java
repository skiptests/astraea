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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.astraea.app.common.DataUnit;
import org.astraea.app.concurrent.Executor;
import org.astraea.app.concurrent.State;

/** Print out the given metrics. */
public class Tracker implements Executor {
  private final List<Metrics> producerData;
  private final List<Metrics> consumerData;
  private final Manager manager;
  private final Supplier<Boolean> producerDone;
  // Snapshot of metrics
  private Result producerResult;
  private Result consumerResult;
  long start = 0L;

  public Tracker(
      List<Metrics> producerData,
      List<Metrics> consumerData,
      Manager manager,
      Supplier<Boolean> producerDone) {
    this.producerData = producerData;
    this.consumerData = consumerData;
    this.producerResult = result(producerData);
    this.consumerResult = result(consumerData);
    this.manager = manager;
    this.producerDone = producerDone;
  }

  @Override
  public State execute() throws InterruptedException {
    producerResult = result(producerData);
    consumerResult = result(consumerData);
    if (logProducers(producerResult) & logConsumers(consumerResult)) return State.DONE;
    // Log after waiting for one second
    Thread.sleep(1000);
    return State.RUNNING;
  }

  Duration duration() {
    if (start == 0L) start = System.currentTimeMillis() - Duration.ofSeconds(1).toMillis();
    return Duration.ofMillis(System.currentTimeMillis() - start);
  }

  static double avg(Duration duration, long value) {
    return duration.toSeconds() <= 0
        ? 0
        : ((double) (value / duration.toSeconds())) / 1024D / 1024D;
  }

  private boolean logProducers(Result result) {
    if (result.completedRecords == 0) return false;

    var duration = duration();
    // Print completion rate (by number of records) or (by time)
    var percentage =
        Math.min(100D, manager.exeTime().percentage(result.completedRecords, duration.toMillis()));

    System.out.println(
        "Time: "
            + duration.toHoursPart()
            + "hr "
            + duration.toMinutesPart()
            + "min "
            + duration.toSecondsPart()
            + "sec");
    System.out.printf("producers completion rate: %.2f%%%n", percentage);
    System.out.printf("  average throughput: %.3f MB/second%n", result.averageBytes(duration));
    System.out.printf(
        "  current throughput: %s/second%n", DataUnit.Byte.of(result.totalCurrentBytes()));
    System.out.printf(
        "  current throughput (real): %s/second%n",
        DataUnit.Byte.of(result.totalCurrentRealBytes()));
    System.out.println("  publish max latency: " + result.maxLatency + " ms");
    System.out.println("  publish mim latency: " + result.minLatency + " ms");
    for (int i = 0; i < result.bytes.size(); ++i) {
      System.out.printf(
          "  producer[%d] average throughput: %.3f MB%n", i, avg(duration, result.bytes.get(i)));
      System.out.printf(
          "  producer[%d] average publish latency: %.3f ms%n", i, result.averageLatencies.get(i));
    }
    System.out.println("\n");
    return producerDone.get();
  }

  private boolean logConsumers(Result result) {
    // there is no consumer, so we just complete this log.
    if (consumerData.isEmpty()) return true;
    if (result.completedRecords == 0) return false;
    var duration = duration();

    // Print out percentage of (consumed records) and (produced records)
    var percentage = result.completedRecords * 100D / manager.producedRecords();
    System.out.printf("consumer completion rate: %.2f%%%n", percentage);
    System.out.printf("  average throughput: %.3f MB/second%n", result.averageBytes(duration));
    System.out.printf(
        "  current throughput: %s/second%n", DataUnit.Byte.of(result.totalCurrentBytes()));
    System.out.printf(
        "  current throughput (real): %s/second%n",
        DataUnit.Byte.of(result.totalCurrentRealBytes()));
    System.out.println("  end-to-end max latency: " + result.maxLatency + " ms");
    System.out.println("  end-to-end mim latency: " + result.minLatency + " ms");
    for (int i = 0; i < result.bytes.size(); ++i) {
      System.out.printf(
          "  consumer[%d] average throughput: %.3f MB%n", i, avg(duration, result.bytes.get(i)));
      System.out.printf(
          "  consumer[%d] average ene-to-end latency: %.3f ms%n",
          i, result.averageLatencies.get(i));
    }
    System.out.println("\n");
    // Target number of records consumed OR consumed all that produced
    return producerDone.get() && percentage >= 100D;
  }

  public Result producerResult() {
    return producerResult;
  }

  public Result consumerResult() {
    return consumerResult;
  }

  private static Result result(List<Metrics> metrics) {
    var completed = 0;
    var bytes = new ArrayList<Long>();
    var currentBytes = new ArrayList<Long>();
    var currentRealBytes = new ArrayList<Long>();
    var averageLatencies = new ArrayList<Double>();
    var max = 0L;
    var min = Long.MAX_VALUE;
    for (Metrics data : metrics) {
      completed += data.num();
      bytes.add(data.bytes());
      currentBytes.add(data.clearAndGetCurrentBytes());
      currentRealBytes.add(data.currentRealBytes());
      averageLatencies.add(data.avgLatency());
      max = Math.max(max, data.max());
      min = Math.min(min, data.min());
    }
    return new Result(
        completed,
        Collections.unmodifiableList(bytes),
        Collections.unmodifiableList(currentBytes),
        Collections.unmodifiableList(currentRealBytes),
        Collections.unmodifiableList(averageLatencies),
        min,
        max);
  }

  static class Result {
    public final long completedRecords;
    public final List<Long> bytes;
    public final List<Long> currentBytes;
    public final List<Long> currentRealBytes;
    public final List<Double> averageLatencies;
    public final long minLatency;
    public final long maxLatency;

    Result(
        long completedRecords,
        List<Long> bytes,
        List<Long> currentBytes,
        List<Long> currentRealBytes,
        List<Double> averageLatencies,
        long minLatency,
        long maxLatency) {
      this.completedRecords = completedRecords;
      this.bytes = bytes;
      this.currentBytes = currentBytes;
      this.currentRealBytes = currentRealBytes;
      this.averageLatencies = averageLatencies;
      this.minLatency = minLatency;
      this.maxLatency = maxLatency;
    }

    double averageBytes(Duration duration) {
      return avg(duration, totalBytes());
    }

    long totalBytes() {
      return bytes.stream().mapToLong(i -> i).sum();
    }

    long totalCurrentBytes() {
      return currentBytes.stream().mapToLong(i -> i).sum();
    }

    Long totalCurrentRealBytes() {
      return currentRealBytes.stream().mapToLong(i -> i).sum();
    }
  }
}
