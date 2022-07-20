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
package org.astraea.app.balancer.utils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.app.balancer.metrics.IdentifiedFetcher;
import org.astraea.app.balancer.metrics.MetricSource;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.partitioner.Configuration;
import org.mockito.Mockito;

public class DummyMetricSource implements MetricSource {

  private final Collection<IdentifiedFetcher> fetchers;

  public DummyMetricSource(
      Configuration configuration, Collection<IdentifiedFetcher> identifiedFetchers) {
    this.fetchers = identifiedFetchers;
  }

  @Override
  public Collection<HasBeanObject> metrics(IdentifiedFetcher fetcher, int brokerId) {
    return List.of();
  }

  @Override
  public Map<IdentifiedFetcher, Map<Integer, Collection<HasBeanObject>>> allBeans() {
    return fetchers.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                fetcher -> fetcher,
                fetcher -> {
                  @SuppressWarnings("unchecked")
                  Map<Integer, Collection<HasBeanObject>> mockedMap = Mockito.mock(Map.class);
                  Mockito.when(mockedMap.get(Mockito.anyInt()))
                      .thenAnswer(
                          invocation -> {
                            return metrics(fetcher, invocation.getArgument(0));
                          });
                  return mockedMap;
                }));
  }

  @Override
  public double warmUpProgress() {
    return 0;
  }

  @Override
  public void awaitMetricReady() {}

  @Override
  public void drainMetrics() {}

  @Override
  public void close() {}
}
