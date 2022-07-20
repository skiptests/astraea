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

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.metrics.jmx.MBeanClient;

public class IdentifiedFetcher implements Fetcher {

  private static final AtomicInteger idCounter = new AtomicInteger();
  private final int id;
  private final Fetcher fetcher;

  public IdentifiedFetcher(Fetcher fetcher) {
    this.id = idCounter.getAndIncrement();
    this.fetcher = fetcher;
  }

  @Override
  public Collection<HasBeanObject> fetch(MBeanClient client) {
    return fetcher.fetch(client);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IdentifiedFetcher that = (IdentifiedFetcher) o;
    return id == that.id && Objects.equals(fetcher, that.fetcher);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, fetcher);
  }
}
