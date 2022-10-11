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
package org.astraea.gui.tab;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.astraea.common.DataSize;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.Utils;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Partition;
import org.astraea.gui.Context;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;

public class TopicTab {

  private static List<Map<String, Object>> beans(List<Partition> partitions, List<Broker> nodes) {
    var topicSize =
        nodes.stream()
            .flatMap(n -> n.folders().stream().flatMap(d -> d.partitionSizes().entrySet().stream()))
            .collect(Collectors.groupingBy(e -> e.getKey().topic()))
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().stream().mapToLong(Map.Entry::getValue).sum()));
    var tps = partitions.stream().collect(Collectors.groupingBy(Partition::topic));
    return tps.keySet().stream()
        .map(
            topic -> {
              var result = new LinkedHashMap<String, Object>();
              result.put("topic", topic);
              result.put("number of partitions", tps.get(topic).size());
              result.put(
                  "number of replicas",
                  tps.get(topic).stream().mapToInt(p -> p.replicas().size()).sum());
              result.put(
                  "size",
                  Optional.ofNullable(topicSize.get(topic))
                      .map(DataSize.Byte::of)
                      .orElse(DataSize.Byte.of(0)));
              result.put(
                  "max timestamp",
                  Utils.format(
                      tps.get(topic).stream()
                          .mapToLong(Partition::maxTimestamp)
                          .max()
                          .orElse(-1L)));
              tps.get(topic).stream()
                  .flatMap(p -> p.replicas().stream())
                  .collect(Collectors.groupingBy(NodeInfo::id))
                  .entrySet()
                  .stream()
                  .sorted(Map.Entry.comparingByKey())
                  .forEach(
                      entry -> result.put("broker:" + entry.getKey(), entry.getValue().size()));
              return result;
            })
        .collect(Collectors.toList());
  }

  public static Tab of(Context context) {
    var pane =
        PaneBuilder.of()
            .searchField("topic name")
            .buttonAction(
                (input, logger) ->
                    context.submit(
                        admin ->
                            admin
                                .topicNames(true)
                                .thenApply(
                                    names ->
                                        names.stream()
                                            .filter(input::matchSearch)
                                            .collect(Collectors.toSet()))
                                .thenCompose(
                                    names ->
                                        admin
                                            .partitions(names)
                                            .thenCombine(admin.brokers(), TopicTab::beans))))
            .build();
    return Tab.of("topic", pane);
  }
}
