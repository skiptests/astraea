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
package org.astraea.app.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.performance.MutableMetric;

/** An interface for polling records. */
public interface Consumer<Key, Value> extends AutoCloseable {

  default Collection<Record<Key, Value>> poll(Duration timeout) {
    return poll(1, timeout);
  }

  /**
   * try to poll data until there are enough records to return or the timeout is reached.
   *
   * @param recordCount max number of returned records.
   * @param timeout max time to wait data
   * @return records
   */
  Collection<Record<Key, Value>> poll(int recordCount, Duration timeout);

  default MutableMetric<Double> getMetric(String metricName) {
    return MutableMetric.create("null", () -> 0.0);
  }

  /**
   * Wakeup the consumer. This method is thread-safe and is useful in particular to abort a long
   * poll. The thread which is blocking in an operation will throw {@link
   * org.apache.kafka.common.errors.WakeupException}. If no thread is blocking in a method which can
   * throw {@link org.apache.kafka.common.errors.WakeupException}, the next call to such a method
   * will raise it instead.
   */
  void wakeup();

  @Override
  void close();

  /**
   * Create a consumer builder by setting specific topics
   *
   * @param topics set of topic names
   * @return consumer builder for topics
   */
  static TopicsBuilder<byte[], byte[]> forTopics(Set<String> topics) {
    return new TopicsBuilder<>(topics);
  }

  /**
   * Create a consumer builder by setting specific topic partitions
   *
   * @param partitions set of TopicPartition
   * @return consumer builder for partitions
   */
  static PartitionsBuilder<byte[], byte[]> forPartitions(Set<TopicPartition> partitions) {
    return new PartitionsBuilder<>(partitions);
  }
}
