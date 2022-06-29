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
package org.astraea.app.admin;

import com.beust.jcommander.Parameter;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.app.argument.DurationField;
import org.astraea.app.argument.NonEmptyStringField;
import org.astraea.app.common.DataRate;
import org.astraea.app.common.DataSize;
import org.astraea.app.common.DataUnit;
import org.astraea.app.common.Utils;

public class ReplicaSyncingMonitor {

  public static void main(String[] args) {
    Argument argument = org.astraea.app.argument.Argument.parse(new Argument(), args);
    try (Admin topicAdmin = Admin.of(argument.bootstrapServers())) {
      execute(topicAdmin, argument);
    }
  }

  static void execute(final Admin topicAdmin, final Argument argument) {

    // this supplier will give you all the topic names that the client is interested in.
    // discover any newly happened non-synced replica can be a quiet useful scenario, so here we use
    // a supplier to do the query for us.
    Supplier<Set<String>> topicToTrack =
        () -> argument.topics.isEmpty() ? topicAdmin.topicNames() : argument.topics;

    // the non-synced topic-partition we want to monitor
    var topicPartitionToTrack = findNonSyncedTopicPartition(topicAdmin, topicToTrack.get());

    // keep tracking the previous replica size of a topic-partition-replica tuple
    final var previousCheckedSize = new HashMap<TopicPartitionReplica, Long>();

    while (!topicPartitionToTrack.isEmpty() || argument.keepTrack) {

      long startTime = System.nanoTime();

      System.out.printf(
          "[%s]%n", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
      var replicaProgress =
          topicAdmin.replicas(
              topicPartitionToTrack.stream()
                  .map(TopicPartition::topic)
                  .collect(Collectors.toUnmodifiableSet()));

      var topicPartitionLeaderReplicaTable =
          replicaProgress.entrySet().stream()
              .map(
                  x ->
                      Map.entry(
                          x.getKey(),
                          x.getValue().stream().filter(Replica::leader).findFirst().orElseThrow()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      topicPartitionToTrack.stream()
          .map(TopicPartition::topic)
          .distinct()
          .sorted()
          .forEachOrdered(
              topic -> {
                var partitionReplicas =
                    replicaProgress.entrySet().stream()
                        .filter(tpr -> tpr.getKey().topic().equals(topic))
                        .filter(tpr -> topicPartitionToTrack.contains(tpr.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                System.out.printf("  Topic \"%s\":%n", topic);

                partitionReplicas.keySet().stream()
                    .map(TopicPartition::partition)
                    .distinct()
                    .sorted()
                    .forEachOrdered(
                        partition -> {
                          TopicPartition tp = new TopicPartition(topic, partition);
                          Replica leaderReplica = topicPartitionLeaderReplicaTable.get(tp);
                          List<Replica> thisReplicas = partitionReplicas.get(tp);

                          // printing info
                          System.out.printf("  | Partition %d:%n", partition);
                          thisReplicas.stream()
                              .map(
                                  replica ->
                                      Map.entry(
                                          new TopicPartitionReplica(
                                              topic, partition, replica.broker()),
                                          replica))
                              .map(
                                  entry ->
                                      Map.entry(
                                          entry.getValue(),
                                          new ProgressInfo(
                                              DataUnit.Byte.of(leaderReplica.size()),
                                              DataUnit.Byte.of(
                                                  previousCheckedSize.getOrDefault(
                                                      entry.getKey(), entry.getValue().size())),
                                              DataUnit.Byte.of(entry.getValue().size()),
                                              argument.interval)))
                              .map(
                                  entry ->
                                      String.format(
                                          "replica on broker %3d => %s",
                                          entry.getKey().broker(),
                                          formatString(entry.getKey(), entry.getValue())))
                              .map(s -> String.format("  | | %s", s))
                              .forEachOrdered(System.out::println);

                          // update previous size
                          thisReplicas.stream()
                              .map(
                                  replica ->
                                      Map.entry(
                                          new TopicPartitionReplica(
                                              topic, partition, replica.broker()),
                                          replica))
                              .forEach(
                                  entry ->
                                      previousCheckedSize.put(
                                          entry.getKey(), entry.getValue().size()));
                        });
              });

      // remove synced topic-partition-replica
      var topicPartitionFinished =
          replicaProgress.entrySet().stream()
              .filter(x -> x.getValue().stream().allMatch(Replica::inSync))
              .map(Map.Entry::getKey)
              .collect(Collectors.toSet());
      topicPartitionToTrack.removeAll(topicPartitionFinished);

      if (topicPartitionToTrack.isEmpty()) {
        System.out.println("  Every replica is synced.");
      }
      System.out.println();

      // attempts to discover any non-synced replica if flag --keep-track is used
      if (argument.keepTrack) {
        // find new non-synced topic-partition
        Set<TopicPartition> nonSyncedTopicPartition =
            findNonSyncedTopicPartition(topicAdmin, topicToTrack.get());
        // add all the non-synced topic-partition into tracking
        topicPartitionToTrack.addAll(nonSyncedTopicPartition);
        // remove previous progress from size map
        previousCheckedSize.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            .entrySet()
            .stream()
            .filter(
                tpr ->
                    !nonSyncedTopicPartition.contains(
                        new TopicPartition(tpr.getKey().topic(), tpr.getKey().partition())))
            .distinct()
            .forEach(tpr -> previousCheckedSize.remove(tpr.getKey()));
      }

      Utils.packException(
          () -> {
            long expectedWaitNs = argument.interval.toNanos();
            long elapsedNs = (System.nanoTime() - startTime);
            TimeUnit.NANOSECONDS.sleep(Math.max(expectedWaitNs - elapsedNs, 0));
            return 0;
          });
    }
  }

  private static String formatString(Replica replica, ProgressInfo value) {
    return Stream.of(
            Optional.ofNullable(value.progressBar()),
            Optional.ofNullable(replica.leader() ? null : value.dataRateString()),
            Optional.ofNullable(
                replica.leader() || replica.inSync()
                    ? null
                    : "(" + value.estimateFinishTimeString() + ")"),
            Optional.ofNullable(replicaDescriptor(replica, value)))
        .flatMap(Optional::stream)
        .collect(Collectors.joining(" "));
  }

  static Set<TopicPartition> findNonSyncedTopicPartition(
      Admin topicAdmin, Set<String> topicToTrack) {
    return topicAdmin.replicas(topicToTrack).entrySet().stream()
        .filter(tpr -> tpr.getValue().stream().anyMatch(replica -> !replica.inSync()))
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }

  static class ProgressInfo {
    private final DataSize leaderSize;
    private final DataSize previousSize;
    private final DataSize currentSize;
    private final Duration interval;

    ProgressInfo(
        DataSize leaderSize, DataSize previousSize, DataSize currentSize, Duration interval) {
      this.leaderSize = leaderSize;
      this.previousSize = previousSize;
      this.currentSize = currentSize;
      this.interval = interval;
    }

    public double progress() {
      var currentBit = currentSize.measurement(DataUnit.Bit).doubleValue();
      var leaderBit = leaderSize.measurement(DataUnit.Bit).doubleValue();
      if (currentBit == leaderBit) return 100;
      // The Admin API doesn't offer any concurrency guarantee for these log size numbers. It is
      // hard ensure the relationship between these two numbers, So we just assume the follower log
      // size might somehow be larger than the leader log.
      // TODO: prove the below condition is impossible to occur so it is safe to remove it.
      if (currentBit > leaderBit) return Double.NaN;
      return currentBit / leaderBit * 100.0;
    }

    public String progressBar() {
      final int totalBlocks = 20;
      final int filledBlocks =
          (int) Math.min(totalBlocks, Math.floor(0.2 + progress() / (100.0 / totalBlocks)));
      final int emptyBlocks = totalBlocks - filledBlocks;

      return String.format(
          "[%s%s] %6.2f%%",
          String.join("", Collections.nCopies(filledBlocks, "#")),
          String.join("", Collections.nCopies(emptyBlocks, " ")),
          progress());
    }

    public DataRate dataRate() {
      if (isProgressFallback()) {
        // log retention/compaction occurred, we don't know the actual data rate at this moment.
        return DataRate.of(0, DataUnit.Byte, interval);
      } else {
        return currentSize.subtract(previousSize).dataRate(interval);
      }
    }

    public double dataRate(DataUnit dataUnit, ChronoUnit chronoUnit) {
      return dataRate().toBigDecimal(dataUnit, chronoUnit).doubleValue();
    }

    boolean isStalled() {
      return currentSize.equals(previousSize);
    }

    /**
     * For some reason the log size decreased. This might occur due to various reasons, for example:
     * log retention/compaction.
     */
    boolean isProgressFallback() {
      return currentSize.compareTo(previousSize) < 0;
    }

    /**
     * For some reason the current log size is smaller than leader size. This might occur due to
     * various reasons, for example: race condition between API fetching data.
     */
    boolean isProgressOverflow() {
      return currentSize.compareTo(leaderSize) > 0;
    }

    /**
     * Estimated the finish time, return a negative duration if we can't infer the actual finished
     * time. This might happen due to the following reason:
     *
     * <ol>
     *   <li>The replica syncing progress stalled.
     *   <li>Log retention/compaction: cause the actual log size changed.
     *   <li>Significant log size changed between replica data RPC call. The size of a log is a
     *       moving target. It is hard to find the actual size without strong synchronization. But
     *       doing that probably hurts performance and offers not much value to the user.
     * </ol>
     *
     * @return a {@link Duration} represent the estimated finish time, negative if progress is
     *     stalled.
     */
    public Duration estimateFinishTime() {
      if (isStalled() || isProgressFallback() || isProgressOverflow())
        return Duration.ofSeconds(-1);
      else {
        final double leaderByte = leaderSize.measurement(DataUnit.Byte).doubleValue();
        final double currentByte = currentSize.measurement(DataUnit.Byte).doubleValue();
        final double estimateFinishTime =
            ((leaderByte - currentByte) / dataRate(DataUnit.Byte, ChronoUnit.SECONDS));
        return Duration.ofSeconds((long) estimateFinishTime);
      }
    }

    public String estimateFinishTimeString() {
      Duration estimatedTime = estimateFinishTime();
      return estimatedTime.isNegative()
          ? "unknown"
          : estimatedTime.toSeconds() == 0
              ? "about now"
              : Stream.of(
                      Map.entry(estimatedTime.toHoursPart(), "h"),
                      Map.entry(estimatedTime.toMinutesPart(), "m"),
                      Map.entry(estimatedTime.toSecondsPart(), "s"))
                  .dropWhile(x -> x.getKey() == 0)
                  .map(x -> x.getKey().toString() + x.getValue())
                  .collect(Collectors.joining(" ", "", " estimated"));
    }

    public String dataRateString() {
      if (isProgressFallback()) {
        return dataRate().toString(ChronoUnit.SECONDS).replaceAll("[+\\-\\d.]+", "?");
      } else {
        return dataRate().toString(ChronoUnit.SECONDS);
      }
    }

    @Override
    public String toString() {
      if (estimateFinishTime().isZero() && isStalled()) return dataRateString();
      else return String.format("%s (%s)", dataRateString(), estimateFinishTimeString());
    }
  }

  static String replicaDescriptor(Replica replica, ProgressInfo progressInfo) {
    var logShrinkage = progressInfo.isProgressFallback() | progressInfo.isProgressOverflow();
    return Stream.of(
            Optional.ofNullable(replica.leader() ? "leader" : null),
            Optional.ofNullable(replica.inSync() ? "synced" : null),
            Optional.ofNullable(replica.lag() > 0 ? "lagged" : null),
            Optional.ofNullable(logShrinkage ? "log-shrunk" : null))
        .flatMap(Optional::stream)
        .collect(Collectors.joining(", ", "[", "]"));
  }

  static class Argument extends org.astraea.app.argument.Argument {

    @Parameter(
        names = {"--topics"},
        description = "String: topics to track, use all non-synced topics by default",
        validateWith = NonEmptyStringField.class)
    public Set<String> topics = Set.of();

    @Parameter(
        names = {"--track"},
        description =
            "Boolean: keep track even if all the replicas are synced, also attempts to discover any non-synced replicas")
    public boolean keepTrack = false;

    @Parameter(
        names = {"--interval"},
        description =
            "Time: the time interval between replica state check, support multiple time unit like 10s, 500ms and 100us. "
                + "If no time unit specified, second unit will be used.",
        validateWith = DurationField.class,
        converter = DurationField.class)
    public Duration interval = Duration.ofSeconds(1);
  }
}
