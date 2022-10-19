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

import java.util.Set;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.consumer.ConsumerRebalanceListener;

public class PartitionRatioListener implements ConsumerRebalanceListener {
  private final String clientId;

  PartitionRatioListener(String clientId) {
    this.clientId = clientId;
  }

  @Override
  public void onPartitionAssigned(Set<TopicPartition> partitions) {
    ConsumerThread.CLIENT_ID_REVOKED_PARTITIONS.putIfAbsent(clientId, Set.of());
    ConsumerThread.CLIENT_ID_ASSIGNED_PARTITIONS.put(clientId, partitions);
  }

  @Override
  public void onPartitionsRevoked(Set<TopicPartition> partitions) {
    ConsumerThread.CLIENT_ID_REVOKED_PARTITIONS.put(clientId, partitions);
  }
}
