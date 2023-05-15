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
package org.astraea.common.assignor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.astraea.common.admin.TopicPartition;

@FunctionalInterface
public interface Generator {

  Map<String, List<TopicPartition>> get();

  static Generator randomGenerator(
      Set<String> consumers, Set<TopicPartition> partitions, Hint hint) {
    return () -> {
      Map<String, List<TopicPartition>> result =
          consumers.stream()
              .map(c -> Map.entry(c, new ArrayList<TopicPartition>()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      List<String> candidates;

      for (var tp : partitions) {
        candidates = hint.get(result, tp);
        if (candidates.isEmpty()) candidates = consumers.stream().toList();

        result.get(candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()))).add(tp);
      }

      return result;
    };
  }
}