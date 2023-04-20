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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import org.astraea.common.ByteUtils;
import org.astraea.common.consumer.Record;

public class RecordReaderBuilder {

  private static final Function<InputStream, RecordReader> V0 =
      inputStream ->
          new RecordReader() {
            private Record<byte[], byte[]> nextCache = null;

            @Override
            public boolean hasNext() {
              // Try to parse a record from the inputStream. And store the parsed record for
              // RecordReader#next().
              if (nextCache == null) {
                nextCache = ByteUtils.readRecord(inputStream);
              }
              // nextCache will be null
              return nextCache != null;
            }

            @Override
            public Record<byte[], byte[]> next() {
              if (hasNext()) {
                var next = nextCache;
                nextCache = null;
                return next;
              }
              throw new NoSuchElementException("RecordReader has no more elements.");
            }
          };

  private InputStream fs;

  RecordReaderBuilder(InputStream inputStream) {
    this.fs = inputStream;
  }

  public RecordReaderBuilder compression() throws IOException {
    this.fs = new GZIPInputStream(this.fs);
    return this;
  }

  public RecordReaderBuilder buffered() {
    this.fs = new BufferedInputStream(this.fs);
    return this;
  }

  public RecordReaderBuilder buffered(int size) {
    this.fs = new BufferedInputStream(this.fs, size);
    return this;
  }

  public RecordReader build() {
    var version = ByteUtils.readShort(fs);
    switch (version) {
      case 0:
        return V0.apply(fs);
      default:
        throw new IllegalArgumentException("unsupported version: " + version);
    }
  }
}
