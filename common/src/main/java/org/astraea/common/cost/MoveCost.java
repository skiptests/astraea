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

import java.util.Map;

/** Return type of cost function, `HasMoveCost`. It returns the score of migrate plan. */
@FunctionalInterface
public interface MoveCost {
  /** @return the function name of MoveCost */
  default String name() {
    return this.getClass().getSimpleName();
  }

  /** @return cost of migrate plan */
  long totalCost();

  /** @return unit of cost */
  default String unit() {
    return "unknown";
  }

  /** @return Changes per broker, negative if brokers moved out, positive if brokers moved in */
  default Map<Integer, Long> changes() {
    return Map.of();
  }
}
