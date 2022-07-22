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

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.app.admin.TopicPartition;

@FunctionalInterface
public interface ConsumerRebalanceListener {

  /**
   * A callback method the user can implement to provide handling of customized offsets on
   * completion of a successful partition re-assignment. See {@link
   * org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(Collection)}
   * for more detail.
   */
  void onPartitionAssigned(Set<TopicPartition> partitions);

  default void onPartitionsRevoked(Set<TopicPartition> partitions) {}
  ;

  static org.apache.kafka.clients.consumer.ConsumerRebalanceListener of(
      ConsumerRebalanceListener listener) {
    return new org.apache.kafka.clients.consumer.ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(Collection<org.apache.kafka.common.TopicPartition> ignore) {
        listener.onPartitionsRevoked(
            ignore.stream().map(TopicPartition::from).collect(Collectors.toSet()));
      }

      @Override
      public void onPartitionsAssigned(
          Collection<org.apache.kafka.common.TopicPartition> partitions) {
        listener.onPartitionAssigned(
            partitions.stream().map(TopicPartition::from).collect(Collectors.toSet()));
      }
    };
  }
}
