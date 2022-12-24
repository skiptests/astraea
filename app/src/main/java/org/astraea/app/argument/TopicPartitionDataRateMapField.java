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
package org.astraea.app.argument;

import com.beust.jcommander.ParameterException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.DataRate;
import org.astraea.common.admin.TopicPartition;

public class TopicPartitionDataRateMapField extends Field<Map<TopicPartition, DataRate>> {
  private static final DataRateField rateConverter = new DataRateField();

  @Override
  public Map<TopicPartition, DataRate> convert(String value) {
    return Arrays.stream(value.split(","))
        .map(
            item -> {
              var k = item.split(":");
              if (k.length != 2) throw new ParameterException("incorrect format : " + item);
              return Map.entry(TopicPartition.of(k[0]), rateConverter.convert(k[1]));
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
