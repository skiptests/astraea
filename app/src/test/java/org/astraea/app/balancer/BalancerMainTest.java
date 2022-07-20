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
package org.astraea.app.balancer;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;
import org.astraea.app.common.Utils;
import org.astraea.app.partitioner.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class BalancerMainTest {

  @Test
  void testExecute() {
    var emptyConfigs = Configuration.of(Map.of());
    try (var mockConstructor = Mockito.mockConstruction(Balancer.class)) {
      BalancerMain.execute(emptyConfigs);
      Mockito.verify(mockConstructor.constructed().get(0), Mockito.times(1)).run();
    }
  }

  @Test
  void testArgument() {
    // arrange
    try (var staticMock = Mockito.mockStatic(BalancerMain.class)) {
      staticMock.when(() -> BalancerMain.main(Mockito.any())).thenCallRealMethod();
      staticMock.when(() -> BalancerMain.configuration(Mockito.any())).thenCallRealMethod();

      // act
      Assertions.assertDoesNotThrow(
          () -> {
            var file = File.createTempFile("config", ".properties");
            var args = new String[] {file.getAbsolutePath()};
            BalancerMain.main(args);
          });

      // assert, the given argument works
      staticMock.verify(() -> BalancerMain.execute(Mockito.any()), Mockito.atLeastOnce());
    }
  }

  @Test
  void testBadArgument() {
    // arrange
    try (var staticMock = Mockito.mockStatic(BalancerMain.class)) {
      staticMock.when(() -> BalancerMain.main(Mockito.any())).thenCallRealMethod();
      staticMock.when(() -> BalancerMain.configuration(Mockito.any())).thenCallRealMethod();

      // act
      Assertions.assertThrows(
          UncheckedIOException.class,
          () -> {
            var args = new String[] {"/path/to/non-exists-file-" + Utils.randomString()};
            BalancerMain.main(args);
          });

      // assert, the given argument doesn't works
      staticMock.verify(() -> BalancerMain.execute(Mockito.any()), Mockito.never());
    }
  }

  @Test
  void testConfiguration() throws IOException {
    // arrange
    var configFile = File.createTempFile("config", ".properties");
    try (var stream = Files.newOutputStream(configFile.toPath())) {
      stream.write("1+1=2\n".getBytes());
      stream.write("knowledge=power\n".getBytes());
      stream.write("time=money\n".getBytes());
      stream.write("rose=red\n".getBytes());
      stream.write("violet=blue\n".getBytes());
      stream.write("sugar=sweet\n".getBytes());
    }

    // act
    var config = BalancerMain.configuration(configFile);

    // assert
    Assertions.assertEquals(6, config.entrySet().size());
    Assertions.assertEquals("2", config.requireString("1+1"));
    Assertions.assertEquals("power", config.requireString("knowledge"));
    Assertions.assertEquals("money", config.requireString("time"));
    Assertions.assertEquals("red", config.requireString("rose"));
    Assertions.assertEquals("blue", config.requireString("violet"));
    Assertions.assertEquals("sweet", config.requireString("sugar"));
  }
}
