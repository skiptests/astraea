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

import java.util.Set;
import org.astraea.app.common.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TopicThrottleSettingTest {

  @Test
  void testAllLogThrottled() {
    var setting = TopicThrottleSetting.allLogThrottled("Some");

    Assertions.assertTrue(setting.allThrottled());
    Assertions.assertFalse(setting.notThrottled());
    Assertions.assertFalse(setting.partialThrottled());
    Assertions.assertEquals(Set.of(), setting.throttledLogs());
    Assertions.assertEquals("Some", setting.topicName());
    Assertions.assertEquals(TopicThrottleSetting.allLogThrottled("Some"), setting);
    Assertions.assertEquals(
        TopicThrottleSetting.allLogThrottled("Some").hashCode(), setting.hashCode());
    Assertions.assertNotEquals(
        TopicThrottleSetting.allLogThrottled("Other").hashCode(), setting.hashCode());
  }

  @Test
  void testPartialLogThrottled() {
    var logs =
        Set.of(
            new TopicPartitionReplica("topicA", 0, 0),
            new TopicPartitionReplica("topicA", 1, 1),
            new TopicPartitionReplica("topicA", 2, 2));
    var setting = TopicThrottleSetting.someLogThrottled(logs);

    Assertions.assertTrue(setting.partialThrottled());
    Assertions.assertFalse(setting.allThrottled());
    Assertions.assertFalse(setting.notThrottled());
    Assertions.assertEquals("topicA", setting.topicName());
    Assertions.assertEquals(logs, setting.throttledLogs());
    Assertions.assertEquals(TopicThrottleSetting.someLogThrottled(logs), setting);
    Assertions.assertEquals(
        TopicThrottleSetting.someLogThrottled(logs).hashCode(), setting.hashCode());
  }

  @Test
  void testIllegalPartialLog() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> TopicThrottleSetting.someLogThrottled(Set.of()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            TopicThrottleSetting.someLogThrottled(
                Set.of(
                    new TopicPartitionReplica("topicA", 0, 0),
                    new TopicPartitionReplica("topicB", 0, 0))),
        "All the given log must belongs to the same topic");
  }

  @Test
  void testNoThrottle() {
    var topicName = Utils.randomString();
    var settings = TopicThrottleSetting.noThrottle(topicName);

    Assertions.assertTrue(settings.notThrottled());
    Assertions.assertFalse(settings.allThrottled());
    Assertions.assertFalse(settings.partialThrottled());
    Assertions.assertEquals(Set.of(), settings.throttledLogs());
    Assertions.assertEquals(topicName, settings.topicName());
    Assertions.assertEquals(TopicThrottleSetting.noThrottle(topicName), settings);
    Assertions.assertEquals(
        TopicThrottleSetting.noThrottle(topicName).hashCode(), settings.hashCode());
  }
}
