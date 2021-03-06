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
package org.astraea.app.balancer.executor;

import java.util.concurrent.CompletableFuture;
import org.astraea.app.admin.TopicPartition;

public class LeaderElectionTask {

  private final TopicPartition topicPartition;
  private final CompletableFuture<Boolean> completableFuture;

  public LeaderElectionTask(RebalanceAdmin admin, TopicPartition topicPartition) {
    this.topicPartition = topicPartition;
    this.completableFuture = admin.waitPreferredLeaderSynced(topicPartition);
  }

  public TopicPartition topicPartition() {
    return topicPartition;
  }

  public CompletableFuture<Boolean> completableFuture() {
    return completableFuture;
  }
}
