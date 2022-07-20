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
package org.astraea.app;

import com.beust.jcommander.ParameterException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.astraea.app.admin.ReplicaCollie;
import org.astraea.app.admin.ReplicaSyncingMonitor;
import org.astraea.app.admin.TopicExplorer;
import org.astraea.app.automation.Automation;
import org.astraea.app.balancer.Balancer;
import org.astraea.app.cost.topic.PartitionScore;
import org.astraea.app.metrics.MetricExplorer;
import org.astraea.app.performance.Performance;
import org.astraea.app.web.WebService;

public class App {
  private static final Map<String, Class<?>> MAIN_CLASSES =
      Map.of(
          "offset", TopicExplorer.class,
          "metrics", MetricExplorer.class,
          "replica", ReplicaCollie.class,
          "score", PartitionScore.class,
          "performance", Performance.class,
          "monitor", ReplicaSyncingMonitor.class,
          "automation", Automation.class,
          "web", WebService.class,
          "balancer", Balancer.class);

  static void execute(Map<String, Class<?>> mains, List<String> args) throws Throwable {

    var usage = "Usage: " + mains.keySet() + " [args ...]";

    if (args.size() < 1) {
      System.err.println(usage);
      return;
    }

    var className = args.get(0);

    if (className.toLowerCase().equals("help")) {
      System.out.println(usage);
      return;
    }

    var targetClass = mains.get(className);

    if (targetClass == null) {
      System.err.println("the application \"" + className + "\" is nonexistent");
      System.err.println(usage);
      return;
    }

    var method = targetClass.getDeclaredMethod("main", String[].class);
    try {
      method.invoke(null, (Object) args.subList(1, args.size()).toArray(String[]::new));
    } catch (InvocationTargetException targetException) {
      // Print out ParameterException, don't throw.
      if (targetException.getTargetException() instanceof ParameterException) {
        System.out.println(targetException.getTargetException().getMessage());
      } else {
        throw targetException.getTargetException();
      }
    }
  }

  public static void main(String[] args) throws Throwable {
    execute(MAIN_CLASSES, Arrays.asList(args));
  }
}
