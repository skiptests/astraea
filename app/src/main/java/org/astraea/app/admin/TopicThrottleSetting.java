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

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public final class TopicThrottleSetting {

  private final String topicName;
  private final boolean isAllLogThrottled;
  private final Set<TopicPartitionReplica> throttledLog;

  public static TopicThrottleSetting allLogThrottled(String topicName) {
    return new TopicThrottleSetting(topicName, true);
  }

  /**
   * Construct a partially throttled config, all the given {@link TopicPartitionReplica} must
   * belongs to the same topic. Otherwise, an exception will be raised.
   */
  public static TopicThrottleSetting someLogThrottled(Set<TopicPartitionReplica> logs) {
    return new TopicThrottleSetting(logs);
  }

  public static TopicThrottleSetting noThrottle(String topicName) {
    return new TopicThrottleSetting(topicName, false);
  }

  private TopicThrottleSetting(String topicName, boolean allThrottled) {
    this.topicName = topicName;
    this.isAllLogThrottled = allThrottled;
    this.throttledLog = Set.of();
  }

  private TopicThrottleSetting(Set<TopicPartitionReplica> throttledLog) {
    String topic =
        throttledLog.stream()
            .findAny()
            .orElseThrow(() -> new IllegalArgumentException("No log specified"))
            .topic();

    if (!throttledLog.stream().allMatch(l -> l.topic().equals(topic)))
      throw new IllegalArgumentException("Some given log belongs to different log");

    this.topicName = topic;
    this.isAllLogThrottled = false;
    this.throttledLog = Set.copyOf(throttledLog);
  }

  public String topicName() {
    return topicName;
  }

  /** @return true if the whole topic is throttled. */
  public boolean allThrottled() {
    return isAllLogThrottled;
  }

  /**
   * @return true if the topic is partially throttled. That is, the throttle only applies to certain
   *     partition at a certain broker. Call {@link TopicThrottleSetting#throttledLog} to retrieve
   *     the throttled logs.
   */
  public boolean partialThrottled() {
    return !isAllLogThrottled && !throttledLog.isEmpty();
  }

  /**
   * @return a set of throttled logs, return an empty set if the whole topic is throttled or there
   *     is no throttle enabled for this topic.
   */
  public Set<TopicPartitionReplica> throttledLogs() {
    return throttledLog;
  }

  /** @return true if this topic is not throttled. */
  public boolean notThrottled() {
    return !isAllLogThrottled && throttledLog.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TopicThrottleSetting that = (TopicThrottleSetting) o;
    return isAllLogThrottled == that.isAllLogThrottled
        && Objects.equals(topicName, that.topicName)
        && Objects.equals(throttledLog, that.throttledLog);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicName, isAllLogThrottled, throttledLog);
  }

  @Override
  public String toString() {
    return "TopicThrottleSetting{"
        + "topicName='"
        + topicName
        + "', "
        + (notThrottled()
            ? "Not throttled"
            : allThrottled()
                ? "All log throttled"
                : "throttledLogs="
                    + throttledLog.stream()
                        .map(x -> x.partition() + ":" + x.brokerId())
                        .collect(Collectors.joining(",")))
        + '}';
  }
}
