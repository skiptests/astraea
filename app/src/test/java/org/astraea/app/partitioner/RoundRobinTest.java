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
package org.astraea.app.partitioner;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RoundRobinTest {

  @Test
  void testSmoothWeighted() {
    var weight = Map.of(0, 10D, 1, 5D);

    var rr = RoundRobin.smoothWeighted(weight);

    // nothing to pick
    Assertions.assertEquals(Optional.empty(), rr.next(Set.of()));
    Assertions.assertEquals(Optional.empty(), rr.next(Set.of(3)));

    // chosen one
    Assertions.assertEquals(Optional.of(0), rr.next(Set.of(0)));
    Assertions.assertEquals(Optional.of(1), rr.next(Set.of(1)));

    // reset
    rr = RoundRobin.smoothWeighted(weight);
    Assertions.assertEquals(Optional.of(0), rr.next(Set.of(0, 1)));
    Assertions.assertEquals(Optional.of(1), rr.next(Set.of(0, 1)));
    Assertions.assertEquals(Optional.of(0), rr.next(Set.of(0, 1)));
    Assertions.assertEquals(Optional.of(0), rr.next(Set.of(0, 1)));
    Assertions.assertEquals(Optional.of(1), rr.next(Set.of(0, 1)));
    Assertions.assertEquals(Optional.of(0), rr.next(Set.of(0, 1)));
  }
}
