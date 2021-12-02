package org.astraea.producer;

import java.util.Map;

/** An interface for sending records. */
public interface Producer<Key, Value> extends AutoCloseable {
  Sender<Key, Value> sender();

  void beginTransaction();

  void commitTransaction();

  void close();

  static Builder<byte[], byte[]> builder() {
    return new Builder<>();
  }

  static Producer<byte[], byte[]> of(String brokers) {
    return builder().brokers(brokers).build();
  }

  static Producer<byte[], byte[]> of(Map<String, Object> configs) {
    return builder().configs(configs).build();
  }
}
