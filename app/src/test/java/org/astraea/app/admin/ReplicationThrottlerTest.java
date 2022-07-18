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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Set;
import org.astraea.app.common.DataRate;
import org.astraea.app.common.DataUnit;
import org.astraea.app.common.Utils;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ReplicationThrottlerTest extends RequireBrokerCluster {

  @ParameterizedTest
  @EnumSource(value = ReplicaType.class)
  void throttleLog(ReplicaType type) {
    // arrange
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topicName = Utils.randomString();
      admin.creator().topic(topicName).create();
      Utils.sleep(Duration.ofMillis(300));

      // all log throttled
      var setting0 = TopicThrottleSetting.allLogThrottled(topicName);
      admin.replicationThrottler().applyLogThrottle(type, setting0);
      Utils.sleep(Duration.ofMillis(300));
      var return0 = admin.replicationThrottler().getThrottleSetting(topicName);
      Assertions.assertTrue(return0.get(type).allThrottled());

      // partial throttled
      var logs =
          Set.of(
              new TopicPartitionReplica(topicName, 0, 0),
              new TopicPartitionReplica(topicName, 0, 1),
              new TopicPartitionReplica(topicName, 0, 2));
      var setting1 = TopicThrottleSetting.someLogThrottled(logs);
      admin.replicationThrottler().applyLogThrottle(type, setting1);
      Utils.sleep(Duration.ofMillis(300));
      var return1 = admin.replicationThrottler().getThrottleSetting(topicName);
      Assertions.assertTrue(return1.get(type).partialThrottled());
      Assertions.assertEquals(logs, return1.get(type).throttledLogs());

      // no throttled
      var setting2 = TopicThrottleSetting.noThrottle(topicName);
      admin.replicationThrottler().applyLogThrottle(type, setting2);
      Utils.sleep(Duration.ofMillis(300));
      var return2 = admin.replicationThrottler().getThrottleSetting(topicName);
      Assertions.assertTrue(return2.get(type).notThrottled());
    }
  }

  @Test
  void brokerBandwidth() {
    // arrange
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic1 = Utils.randomString();
      var topic2 = Utils.randomString();
      var topic3 = Utils.randomString();
      var rate1 = DataRate.of(100, DataUnit.MiB, ChronoUnit.SECONDS);
      var rate2 = DataRate.of(50, DataUnit.MiB, ChronoUnit.SECONDS);
      var rate3 = DataRate.of(25, DataUnit.MiB, ChronoUnit.SECONDS);
      var rate4 = DataRate.of(10, DataUnit.MiB, ChronoUnit.SECONDS);
      admin.creator().topic(topic1).create();
      admin.creator().topic(topic2).create();
      admin.creator().topic(topic3).create();
      Utils.sleep(Duration.ofMillis(300));

      // set
      admin
          .replicationThrottler()
          .limitBrokerBandwidth(
              Map.of(
                  0, BrokerThrottleRate.onlyFollower(rate1),
                  1, BrokerThrottleRate.onlyLeader(rate2),
                  2, BrokerThrottleRate.of(rate3, rate4)));
      Utils.sleep(Duration.ofSeconds(1));
      var throttleBandwidth0 = admin.replicationThrottler().getThrottleBandwidth(0);
      var throttleBandwidth1 = admin.replicationThrottler().getThrottleBandwidth(1);
      var throttleBandwidth2 = admin.replicationThrottler().getThrottleBandwidth(2);
      Assertions.assertTrue(throttleBandwidth0.leaderThrottle().isEmpty());
      Assertions.assertTrue(throttleBandwidth0.followerThrottle().isPresent());
      Assertions.assertTrue(throttleBandwidth1.leaderThrottle().isPresent());
      Assertions.assertTrue(throttleBandwidth1.followerThrottle().isEmpty());
      Assertions.assertTrue(throttleBandwidth2.leaderThrottle().isPresent());
      Assertions.assertTrue(throttleBandwidth2.followerThrottle().isPresent());
      var ratePerSec1 = rate1.toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS);
      var ratePerSec2 = rate2.toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS);
      var ratePerSec3 = rate3.toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS);
      var ratePerSec4 = rate4.toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS);
      Assertions.assertEquals(
          ratePerSec1,
          throttleBandwidth0
              .followerThrottle()
              .orElseThrow()
              .toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS));
      Assertions.assertEquals(
          ratePerSec2,
          throttleBandwidth1
              .leaderThrottle()
              .orElseThrow()
              .toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS));
      Assertions.assertEquals(
          ratePerSec3,
          throttleBandwidth2
              .leaderThrottle()
              .orElseThrow()
              .toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS));
      Assertions.assertEquals(
          ratePerSec4,
          throttleBandwidth2
              .followerThrottle()
              .orElseThrow()
              .toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS));

      // clear
      admin
          .replicationThrottler()
          .limitBrokerBandwidth(Map.of(0, BrokerThrottleRate.noRateLimit()));
      Utils.sleep(Duration.ofSeconds(1));
      var throttleBandwidth4 = admin.replicationThrottler().getThrottleBandwidth(0);
      Assertions.assertTrue(throttleBandwidth4.leaderThrottle().isEmpty());
      Assertions.assertTrue(throttleBandwidth4.followerThrottle().isEmpty());
    }
  }
}
