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

import java.time.Duration;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.it.Service;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SensorHandlerTest {
  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @Test
  void testBeans() {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(10).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));
      var sensors = new WebService.Sensors();
      var defaultCostHandler = new SensorHandler(sensors);
      var defaultCostResponse =
          Assertions.assertInstanceOf(
              SensorHandler.Response.class,
              defaultCostHandler.get(Channel.EMPTY).toCompletableFuture().join());
      Assertions.assertEquals(3, defaultCostResponse.costs.size());

      var changedCostResponse =
          Assertions.assertInstanceOf(
              SensorHandler.Response.class,
              defaultCostHandler
                  .post(
                      Channel.builder()
                          .request("{\"costs\": [\"org.astraea.common.cost.ReplicaLeaderCost\"]}")
                          .build())
                  .toCompletableFuture()
                  .join());
      Assertions.assertEquals(1, changedCostResponse.costs.size());

      var changedCostGetResponse =
          Assertions.assertInstanceOf(
              SensorHandler.Response.class,
              defaultCostHandler.get(Channel.EMPTY).toCompletableFuture().join());
      Assertions.assertEquals(1, changedCostGetResponse.costs.size());
    }
  }
}
