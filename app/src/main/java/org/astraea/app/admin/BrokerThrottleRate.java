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
import java.util.Optional;
import org.astraea.app.common.DataRate;

public final class BrokerThrottleRate {

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private Optional<DataRate> leaderThrottle;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private Optional<DataRate> followerThrottle;

  public static BrokerThrottleRate noRateLimit() {
    return new BrokerThrottleRate(Optional.empty(), Optional.empty());
  }

  public static BrokerThrottleRate onlyLeader(DataRate leaderThrottle) {
    return new BrokerThrottleRate(Optional.of(leaderThrottle), Optional.empty());
  }

  public static BrokerThrottleRate onlyFollower(DataRate followerThrottle) {
    return new BrokerThrottleRate(Optional.empty(), Optional.of(followerThrottle));
  }

  public static BrokerThrottleRate of(DataRate leaderThrottle, DataRate followerThrottle) {
    return new BrokerThrottleRate(
        Optional.ofNullable(leaderThrottle), Optional.ofNullable(followerThrottle));
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  BrokerThrottleRate(Optional<DataRate> leaderThrottle, Optional<DataRate> followerThrottle) {
    this.leaderThrottle = leaderThrottle;
    this.followerThrottle = followerThrottle;
  }

  public Optional<DataRate> leaderThrottle() {
    return leaderThrottle;
  }

  public Optional<DataRate> followerThrottle() {
    return followerThrottle;
  }

  @Override
  public String toString() {
    return "BrokerThrottleRate{"
        + "leaderThrottle="
        + leaderThrottle
        + ", followerThrottle="
        + followerThrottle
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BrokerThrottleRate that = (BrokerThrottleRate) o;
    return Objects.equals(leaderThrottle, that.leaderThrottle)
        && Objects.equals(followerThrottle, that.followerThrottle);
  }

  @Override
  public int hashCode() {
    return Objects.hash(leaderThrottle, followerThrottle);
  }
}
