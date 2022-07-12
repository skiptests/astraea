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

import java.util.function.Supplier;

/**
 * A metric that the value may change over time. The changes may be caused by other thread, or
 * change when metricValue() is called.
 */
public interface MutableMetric<T> {
  /** Default return type is double. */
  static MutableMetric<Double> of(org.apache.kafka.common.Metric kafkaMetric) {
    return new MutableMetric<>() {
      @Override
      public String metricName() {
        return kafkaMetric.metricName().name();
      }

      @Override
      public Double metricValue() {
        return Double.parseDouble(kafkaMetric.metricValue().toString());
      }
    };
  }

  static <T> MutableMetric<T> create(String name, Supplier<T> metricValue) {
    return new MutableMetric<>() {
      @Override
      public String metricName() {
        return name;
      }

      @Override
      public T metricValue() {
        return metricValue.get();
      }
    };
  }

  String metricName();

  T metricValue();
}
