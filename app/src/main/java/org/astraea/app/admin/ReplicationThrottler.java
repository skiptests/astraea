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

import java.util.Map;

/**
 * Offer a friendly interface to throttle replication bandwidth. See the proposal and discussion at
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-73+Replication+Quotas">KIP-73</a>
 * for further detail.
 */
public interface ReplicationThrottler {

  /**
   * Apply the throttle setting to a specific topic. The existing setting for this topic will be
   * overwritten. This API only declares which log should be throttled. The user has to call {@link
   * ReplicationThrottler#limitBrokerBandwidth(Map)} to specify the actual replication bandwidth
   * that can be used for the specific broker.
   *
   * @param type indicates the throttle setting should be applied at the leader or follower side.
   * @param setting the throttle setting to apply
   */
  void applyLogThrottle(ReplicaType type, TopicThrottleSetting setting);

  /**
   * Specify the maximum bandwidth used for replication for a given broker.
   *
   * @param brokerThrottleRateMap indicates which broker(id) should be throttled at what rate.
   */
  void limitBrokerBandwidth(Map<Integer, BrokerThrottleRate> brokerThrottleRateMap);

  Map<ReplicaType, TopicThrottleSetting> getThrottleSetting(String topicName);

  BrokerThrottleRate getThrottleBandwidth(int brokerIds);
}
