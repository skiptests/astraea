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
package org.astraea.common;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LazyTest {

  @Test
  void testGetWithoutNoDefaultSupplier() {
    var lazy = Lazy.of();
    Assertions.assertThrows(NullPointerException.class, lazy::get);
  }

  @Test
  void testGetWithNllSupplier() {
    var lazy = Lazy.of();
    Assertions.assertThrows(NullPointerException.class, () -> lazy.get(null));
  }

  @Test
  void testCountOfGet() {
    var count = new AtomicInteger();
    Supplier<String> s =
        () -> {
          count.incrementAndGet();
          return "ss";
        };
    var lazy = Lazy.of(s);
    IntStream.range(0, 10)
        .mapToObj(
            ignored -> CompletableFuture.runAsync(() -> Assertions.assertEquals("ss", lazy.get())))
        .forEach(CompletableFuture::join);
    Assertions.assertEquals(1, count.get());
  }
}
