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

import org.astraea.common.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HandlerTest {

  @Test
  void testException() {
    var exception = new IllegalStateException("hello");
    Handler handler =
        (channel) -> {
          throw exception;
        };
    var r =
        Assertions.assertInstanceOf(
            Response.ResponseImpl.class,
            Utils.packException(
                () ->
                    handler
                        .process(Channel.builder().type(Channel.Type.GET).build())
                        .toCompletableFuture()
                        .get()));
    Assertions.assertNotEquals(200, r.code);
    Assertions.assertEquals(exception.getMessage(), r.message);
  }
}
