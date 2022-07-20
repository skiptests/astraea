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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Properties;
import java.util.stream.Collectors;
import org.astraea.app.partitioner.Configuration;

public class BalancerMain {

  public static void execute(Configuration configuration) {
    printConfig(configuration);
    try (Balancer balancer = new Balancer(configuration)) {
      balancer.run();
    }
  }

  public static void main(String[] args) {
    var argument = org.astraea.app.argument.Argument.parse(new Argument(), args);
    execute(configuration(argument.configuration));
  }

  // visible for test
  static Configuration configuration(File configFile) {
    try (FileInputStream configStream = new FileInputStream(configFile)) {
      var properties = new Properties();
      properties.load(configStream);
      return Configuration.of(
          properties.entrySet().stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      e -> e.getKey().toString(), e -> e.getValue().toString())));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void printConfig(Configuration configuration) {
    System.out.println("[Configuration]");
    configuration.entrySet().stream()
        .sorted()
        .forEach(entry -> System.out.printf("%s=%s%n", entry.getKey(), entry.getValue()));
  }

  public static class Argument {

    @Parameter(
        description = "Path to the properties file for balancer",
        converter = FileConverter.class,
        required = true)
    File configuration;
  }
}
