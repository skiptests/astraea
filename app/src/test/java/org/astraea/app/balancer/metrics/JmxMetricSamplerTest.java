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
package org.astraea.app.balancer.metrics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.astraea.app.balancer.BalancerConfigs;
import org.astraea.app.balancer.BalancerConfigsTest;
import org.astraea.app.common.Utils;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.jmx.BeanObject;
import org.astraea.app.metrics.jmx.MBeanClient;
import org.astraea.app.partitioner.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class JmxMetricSamplerTest {

  @FunctionalInterface
  private interface SamplerConsumer {
    void execute(
        JmxMetricSampler jmxMetricSampler, Set<Integer> brokerId, List<IdentifiedFetcher> fetcher)
        throws Exception;
  }

  private BeanObject numberedBean(int num) {
    return new BeanObject(Integer.toString(num), Map.of(), Map.of());
  }

  private void withMockedSampler(Map<String, String> config, SamplerConsumer execution) {
    // variables
    var brokerId = 5566;
    var counter = new AtomicInteger();

    // fake bean & fetcher
    var aBean = (HasBeanObject) () -> numberedBean(counter.getAndIncrement());
    var fetcher = new IdentifiedFetcher((client) -> List.of(aBean));

    // if no jmxServers specified, some default value put in
    var customConfig = new HashMap<>(config);
    customConfig.putIfAbsent(
        BalancerConfigs.JMX_SERVERS_CONFIG, BalancerConfigsTest.jmxString(brokerId, "host0", 5566));
    var balancerConfig = new BalancerConfigs(Configuration.of(customConfig));

    // mock the MBeanClient logic
    try (var mockStatic = Mockito.mockStatic(MBeanClient.class)) {
      mockStatic
          .when(() -> MBeanClient.of(Mockito.any()))
          .thenReturn(Mockito.mock(MBeanClient.class));

      // initialize the testing target
      try (var sampler = new JmxMetricSampler(balancerConfig, List.of(fetcher))) {
        // run
        Utils.packException(
            () ->
                execution.execute(sampler, balancerConfig.jmxServers().keySet(), List.of(fetcher)));
      }
    }
  }

  @Test
  void testMetrics() {
    var config = Map.of(BalancerConfigs.METRICS_SCRAPING_INTERVAL_MS_CONFIG, "100");
    withMockedSampler(
        config,
        (sampler, brokerId, fetcher) -> {
          TimeUnit.MILLISECONDS.sleep(500);
          Assertions.assertTrue(
              sampler.metrics(fetcher.iterator().next(), brokerId.iterator().next()).stream()
                  .map(HasBeanObject::beanObject)
                  .map(BeanObject::domainName)
                  .map(Integer::parseInt)
                  .collect(Collectors.toSet())
                  .containsAll(Set.of(0, 1, 2, 3, 4)));
        });
  }

  @Test
  void warmUpProgress() {
    var config =
        Map.of(
            BalancerConfigs.METRICS_WARM_UP_COUNT_CONFIG, "4",
            BalancerConfigs.METRICS_SCRAPING_INTERVAL_MS_CONFIG, "1000");
    withMockedSampler(
        config,
        (sampler, brokerId, fetcher) -> {
          TimeUnit.MILLISECONDS.sleep(100);
          Assertions.assertEquals(0.25, sampler.warmUpProgress());
          TimeUnit.MILLISECONDS.sleep(1000);
          Assertions.assertEquals(0.50, sampler.warmUpProgress());
          TimeUnit.MILLISECONDS.sleep(1000);
          Assertions.assertEquals(0.75, sampler.warmUpProgress());
          TimeUnit.MILLISECONDS.sleep(1000);
          Assertions.assertEquals(1.00, sampler.warmUpProgress());
        });
  }

  @Test
  void testAwaitMetricReady() {
    var config =
        Map.of(
            BalancerConfigs.METRICS_WARM_UP_COUNT_CONFIG, "10",
            BalancerConfigs.METRICS_SCRAPING_INTERVAL_MS_CONFIG, "100");
    withMockedSampler(
        config,
        (sampler, brokerId, fetcher) -> {
          Assertions.assertNotEquals(1.0, sampler.warmUpProgress());
          sampler.awaitMetricReady();
          Assertions.assertEquals(1.0, sampler.warmUpProgress());
        });
  }

  @Test
  void testDrainMetrics() {
    var config = Map.of(BalancerConfigs.METRICS_SCRAPING_INTERVAL_MS_CONFIG, "1000");
    withMockedSampler(
        config,
        (sampler, brokerId, fetcher) -> {
          var aFetcher = fetcher.iterator().next();
          var aBroker = brokerId.iterator().next();
          TimeUnit.SECONDS.sleep(1);
          Assertions.assertTrue(sampler.metrics(aFetcher, aBroker).size() > 0);
          sampler.drainMetrics();
          Assertions.assertEquals(0, sampler.metrics(aFetcher, aBroker).size());
        });
  }

  @Test
  void testClose() {
    withMockedSampler(
        Map.of(),
        (sampler, ignore0, ignore1) -> {
          // act
          sampler.close();

          // assert
          Assertions.assertDoesNotThrow(sampler::close);
          Assertions.assertThrows(IllegalStateException.class, () -> sampler.metrics(null, 0));
          Assertions.assertThrows(
              IllegalStateException.class, () -> sampler.metrics(Set.of(), null));
          Assertions.assertThrows(IllegalStateException.class, sampler::drainMetrics);
          Assertions.assertThrows(IllegalStateException.class, sampler::awaitMetricReady);
          Assertions.assertThrows(IllegalStateException.class, sampler::warmUpProgress);
        });
  }
}
