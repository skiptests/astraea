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

import com.beust.jcommander.Parameter;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import org.astraea.app.admin.Admin;
import org.astraea.app.argument.NonNegativeIntegerField;
import org.astraea.app.argument.StringMapField;

public class WebService {

  public static void main(String[] args) throws Exception {
    execute(org.astraea.app.argument.Argument.parse(new Argument(), args));
  }

  private static void execute(Argument arg) throws IOException {
    var server = HttpServer.create(new InetSocketAddress(arg.port), 0);
    server.createContext("/topics", new TopicHandler(Admin.of(arg.configs())));
    server.createContext("/groups", new GroupHandler(Admin.of(arg.configs())));
    server.createContext("/brokers", new BrokerHandler(Admin.of(arg.configs())));
    server.createContext("/producers", new ProducerHandler(Admin.of(arg.configs())));
    server.createContext("/quotas", new QuotaHandler(Admin.of(arg.configs())));
    server.createContext("/pipelines", new PipelineHandler(Admin.of(arg.configs())));
    server.createContext("/transactions", new TransactionHandler(Admin.of(arg.configs())));
    if (arg.needJmx())
      server.createContext("/beans", new BeanHandler(Admin.of(arg.configs()), arg.jmxPorts()));
    server.createContext(
        "/records", new RecordHandler(Admin.of(arg.configs()), arg.bootstrapServers()));
    server.createContext("/reassignments", new ReassignmentHandler(Admin.of(arg.configs())));
    server.start();
  }

  static class Argument extends org.astraea.app.argument.Argument {
    @Parameter(
        names = {"--port"},
        description = "Integer: the port to bind",
        validateWith = NonNegativeIntegerField.class,
        converter = NonNegativeIntegerField.class)
    int port = 8001;

    @Parameter(
        names = {"--jmx.port"},
        description = "Integer: the port to query JMX for each server",
        validateWith = NonNegativeIntegerField.class,
        converter = NonNegativeIntegerField.class)
    int jmxPort = -1;

    @Parameter(
        names = {"--jmx.ports"},
        description = "Map: the jmx port for each node. For example: 192.168.50.2=19999",
        validateWith = StringMapField.class,
        converter = StringMapField.class)
    Map<String, String> jmxPorts = Map.of();

    boolean needJmx() {
      return jmxPort > 0 || !jmxPorts.isEmpty();
    }

    Function<String, Integer> jmxPorts() {
      return name ->
          Optional.of(jmxPorts.getOrDefault(name, String.valueOf(jmxPort)))
              .map(Integer::valueOf)
              .filter(i -> i > 0)
              .orElseThrow(() -> new NoSuchElementException(name + " has no jmx port"));
    }
  }
}
