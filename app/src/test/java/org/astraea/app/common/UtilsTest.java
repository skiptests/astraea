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
package org.astraea.app.common;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.app.cost.CostFunction;
import org.astraea.app.partitioner.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class UtilsTest {

  @Test
  void testHandleException() {
    var executionRuntimeException =
        Assertions.assertThrows(
            ExecutionRuntimeException.class,
            () ->
                Utils.packException(
                    () -> {
                      throw new ExecutionException(new IllegalArgumentException());
                    }));

    Assertions.assertEquals(
        IllegalArgumentException.class, executionRuntimeException.getRootCause().getClass());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            Utils.packException(
                () -> {
                  throw new IllegalArgumentException();
                }));

    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            Utils.packException(
                () -> {
                  throw new IOException();
                }));
  }

  @Test
  void testCollectToTreeMap() {
    Assertions.assertInstanceOf(
        SortedMap.class,
        IntStream.range(0, 100).boxed().collect(Utils.toSortedMap(i -> i, i -> i)));
    //noinspection ResultOfMethodCallIgnored
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            Stream.of(Map.entry(1, "hello"), Map.entry(1, "world"))
                .collect(Utils.toSortedMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  @Test
  void testSequence() {
    var future1 = CompletableFuture.supplyAsync(() -> 1);
    var future2 = CompletableFuture.supplyAsync(() -> 2);

    Assertions.assertEquals(Utils.sequence(List.of(future1, future2)).join(), List.of(1, 2));
  }

  @Test
  void testNonEmpty() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> Utils.requireNonEmpty(""));
    Assertions.assertThrows(NullPointerException.class, () -> Utils.requireNonEmpty(null));
  }

  public static class TestConfigCostFunction implements CostFunction {
    public TestConfigCostFunction(Configuration configuration) {}
  }

  public static class TestCostFunction implements CostFunction {
    public TestCostFunction() {}
  }

  public static class TestBadCostFunction implements CostFunction {
    public TestBadCostFunction(int value) {}
  }

  @ParameterizedTest
  @ValueSource(classes = {TestCostFunction.class, TestConfigCostFunction.class})
  void constructCostFunction(Class<? extends CostFunction> aClass) {
    // arrange
    var config = Configuration.of(Map.of());

    // act
    var costFunction = Utils.constructCostFunction(aClass, config);

    // assert
    Assertions.assertInstanceOf(CostFunction.class, costFunction);
    Assertions.assertInstanceOf(aClass, costFunction);
  }

  @Test
  void constructCostFunctionException() {
    // arrange
    var aClass = TestBadCostFunction.class;
    var config = Configuration.of(Map.of());

    // act, assert
    Assertions.assertThrows(
        RuntimeException.class, () -> Utils.constructCostFunction(aClass, config));
  }

  @Test
  void testSwallowException() {
    Assertions.assertDoesNotThrow(
        () ->
            Utils.swallowException(
                () -> {
                  throw new IllegalArgumentException();
                }));
  }

  @Test
  void testIgnoreCaseEnum() {
    Function<String, MyTestEnum> getMyTestEnum =
        x -> Utils.ofIgnoreCaseEnum(MyTestEnum.values(), MyTestEnum::metricsName, x);

    Assertions.assertEquals(MyTestEnum.APPLE, getMyTestEnum.apply("apple"));
    Assertions.assertEquals(MyTestEnum.APPLE, getMyTestEnum.apply("Apple"));
    Assertions.assertEquals(MyTestEnum.APPLE, getMyTestEnum.apply("appLe"));

    Assertions.assertEquals(MyTestEnum.BANANA, getMyTestEnum.apply("banana"));
    Assertions.assertEquals(MyTestEnum.BANANA, getMyTestEnum.apply("Banana"));

    Assertions.assertEquals(MyTestEnum.CAT_CAT, getMyTestEnum.apply("cat_cat"));
    Assertions.assertEquals(MyTestEnum.CAT_CAT, getMyTestEnum.apply("Cat_Cat"));
  }

  private enum MyTestEnum {
    APPLE("Apple"),
    BANANA("banana"),
    CAT_CAT("CAT_CAT");
    private final String metricsName;

    MyTestEnum(String metricsName) {
      this.metricsName = metricsName;
    }

    public String metricsName() {
      return metricsName;
    }
  }
}
