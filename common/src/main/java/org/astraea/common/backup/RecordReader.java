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
package org.astraea.common.backup;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import org.astraea.common.consumer.Header;
import org.astraea.common.consumer.Record;

public interface RecordReader {

  static Iterator<Record<byte[], byte[]>> read(File file) {
    try (var reader = new FileInputStream(file)) {
      var channel = reader.getChannel();
      var version = ByteBufferUtils.readShort(channel);
      switch (version) {
        case 0:
          return readV0(channel);
        default:
          throw new IllegalArgumentException("unsupported version: " + version);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Iterator<Record<byte[], byte[]>> readV0(SeekableByteChannel channel)
      throws IOException {
    var records = new ArrayList<Record<byte[], byte[]>>();
    var current = channel.position();
    var count = ByteBufferUtils.readInt(channel.position(channel.size() - Integer.BYTES));
    channel.position(current);
    for (var i = 0; i != count; ++i) {
      var recordSize = ByteBufferUtils.readInt(channel);
      var recordBuffer = ByteBuffer.allocate(recordSize);
      var actualSize = channel.read(recordBuffer);
      if (actualSize != recordSize)
        throw new IllegalStateException(
            "expected size is " + recordSize + ", but actual size is " + actualSize);
      recordBuffer.flip();
      var topic = ByteBufferUtils.readString(recordBuffer, recordBuffer.getShort());
      var partition = recordBuffer.getInt();
      var timestamp = recordBuffer.getLong();
      var key = ByteBufferUtils.readBytes(recordBuffer, recordBuffer.getInt());
      var value = ByteBufferUtils.readBytes(recordBuffer, recordBuffer.getInt());
      var headers = new ArrayList<Header>();
      var headerCnt = recordBuffer.getInt();
      for (int headerIndex = 0; headerIndex < headerCnt; headerIndex++) {
        headers.add(
            Header.of(
                Arrays.toString(ByteBufferUtils.readBytes(recordBuffer, recordBuffer.getShort())),
                ByteBufferUtils.readBytes(recordBuffer, recordBuffer.getInt())));
      }
      // TODO: need builder
      records.add(
          new Record<>(
              topic,
              partition,
              0L,
              timestamp,
              key.length,
              0,
              headers,
              key,
              value,
              Optional.empty()));
    }
    return records.iterator();
  }
}
