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
package org.astraea.common.cost;

import static org.astraea.common.cost.MigrationCost.brokerMaxRate;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.astraea.common.Configuration;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.Sensor;
import org.astraea.common.metrics.broker.HasMaxRate;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.collector.MetricSensor;
import org.astraea.common.metrics.stats.Max;

/** MoveCost: more max write rate change -> higher migrate cost. */
public class MigrateTimeCost implements HasMoveCost {
  static final String REPLICATION_RATE = "replication_rate";
  static final String MAX_MIGRATE_TIME_KEY = "max.migrated.time.limit";

  // metrics windows size
  private final Duration maxMigrateTime;

  public MigrateTimeCost(Configuration config) {
    this.maxMigrateTime =
        config.duration(MAX_MIGRATE_TIME_KEY).orElse(Duration.ofSeconds(Long.MAX_VALUE));
  }

  @Override
  public MetricSensor metricSensor() {
    return (client, clusterBean) -> {
      var oldInRate =
          brokerMaxRate(
              client.identity(), clusterBean, MigrateTimeCost.MaxReplicationInRateBean.class);
      var oldOutRate =
          brokerMaxRate(
              client.identity(), clusterBean, MigrateTimeCost.MaxReplicationOutRateBean.class);
      var newInMetrics = ServerMetrics.BrokerTopic.REPLICATION_BYTES_IN_PER_SEC.fetch(client);
      var newOutMetrics = ServerMetrics.BrokerTopic.REPLICATION_BYTES_OUT_PER_SEC.fetch(client);
      var current = Duration.ofMillis(System.currentTimeMillis());
      var maxInRateSensor = Sensor.builder().addStat(REPLICATION_RATE, Max.<Double>of()).build();
      var maxOutRateSensor = Sensor.builder().addStat(REPLICATION_RATE, Max.<Double>of()).build();
      maxInRateSensor.record(newInMetrics.oneMinuteRate());
      maxOutRateSensor.record(newOutMetrics.oneMinuteRate());
      var inRate = maxInRateSensor.measure(REPLICATION_RATE);
      var outRate = maxOutRateSensor.measure(REPLICATION_RATE);
      return List.of(
          new MaxReplicationInRateBean(
              new BeanObject(
                  newInMetrics.beanObject().domainName(),
                  newInMetrics.beanObject().properties(),
                  Map.of(REPLICATION_RATE, Math.max(oldInRate.orElse(0.0), inRate)),
                  current.toMillis())),
          new MaxReplicationOutRateBean(
              new BeanObject(
                  newOutMetrics.beanObject().domainName(),
                  newOutMetrics.beanObject().properties(),
                  Map.of(REPLICATION_RATE, Math.max(oldOutRate.orElse(0.0), outRate)),
                  current.toMillis())));
    };
  }

  @Override
  public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    var brokerMigrateSecond = MigrationCost.brokerMigrationSecond(before, after, clusterBean);
    var planMigrateSecond =
        brokerMigrateSecond.values().stream()
            .max(Comparator.comparing(Function.identity()))
            .orElse(Long.MAX_VALUE);
    return () -> planMigrateSecond > this.maxMigrateTime.getSeconds();
  }

  public record MaxReplicationInRateBean(BeanObject beanObject) implements HasMaxRate {
    @Override
    public BeanObject beanObject() {
      return beanObject;
    }
  }

  public record MaxReplicationOutRateBean(BeanObject beanObject) implements HasMaxRate {
    @Override
    public BeanObject beanObject() {
      return beanObject;
    }
  }
}
