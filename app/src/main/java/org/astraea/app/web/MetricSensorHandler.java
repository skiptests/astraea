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
package org.astraea.app.web;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.astraea.app.web.WebService.MetricSensors;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.cost.CostFunction;
import org.astraea.common.json.TypeRef;

public class MetricSensorHandler implements Handler {

  private final MetricSensors metricSensors;
  private static final Set<String> DEFAULT_COSTS =
      Set.of(
          "org.astraea.common.cost.ReplicaLeaderCost",
          "org.astraea.common.cost.NetworkIngressCost");

  MetricSensorHandler(MetricSensors metricSensors) {
    this.metricSensors = metricSensors;
  }

  @Override
  public CompletionStage<Response> get(Channel channel) {
    var costs =
        metricSensors.metricSensors().isEmpty()
            ? DEFAULT_COSTS
            : metricSensors.metricSensors().stream()
                .map(x -> x.getClass().getName())
                .collect(Collectors.toSet());
    return CompletableFuture.completedFuture(new Response(costs));
  }

  @Override
  public CompletionStage<Response> post(Channel channel) {
    var metricSensorPostRequest = channel.request(TypeRef.of(MetricSensorPostRequest.class));
    var costs = costs(metricSensorPostRequest.costs);
    metricSensors.clearSensors();
    costs.forEach(costFunction -> costFunction.metricSensor().ifPresent(metricSensors::addSensors));
    return CompletableFuture.completedFuture(new Response(metricSensorPostRequest.costs));
  }

  private static Set<CostFunction> costs(Set<String> costs) {
    if (costs.isEmpty()) throw new IllegalArgumentException("costs is not specified");
    return Utils.costFunctions(costs, CostFunction.class, Configuration.EMPTY);
  }

  static class MetricSensorPostRequest implements Request {
    Set<String> costs = DEFAULT_COSTS;
  }

  static class Response implements org.astraea.app.web.Response {
    final Set<String> costs;

    Response(Set<String> costs) {
      this.costs = costs;
    }
  }
}
