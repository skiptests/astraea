package org.astraea.partitioner.dependency;

import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.astraea.topic.TopicAdmin;

/**
 * Each group of order records uses the same DependencyRecord instance. A DependencyRecord will only
 * maintain an immutable partition.
 *
 * <pre>{@code
 * DependencyRecord dependencyRecord = new DependencyRecord();
 * KafkaProducer<String, String> producer = new KafkaProducer<>(props);
 * producer.send(dependencyRecord.orderRecord());
 * producer.send(dependencyRecord.orderRecord());
 * producer.send(dependencyRecord.orderRecord());
 * }</pre>
 *
 * Users can also directly obtain the target partition and send it by themselves.
 */
public class DependencyRecord<K, V> {
  private final int partition;

  public DependencyRecord(KafkaProducer<?, ?> producer, Properties props, String topicName) {
    try (var topicAdmin =
        TopicAdmin.of(props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
      var topic = Set.of(topicName);
      var offsets = topicAdmin.offsets(topic);
      if (offsets.size() == 1) {
        this.partition = offsets.keySet().stream().findFirst().get().partition();
      } else {
        var loadComparison = new LoadComparison(producer, offsets, topicAdmin, topic, props);
        this.partition = loadComparison.lessLoad();
      }
    }
  }

  /**
   * Create a record to be sent to Kafka
   *
   * @param topic The topic the record will be appended to
   * @param key The key that will be included in the record
   * @param value The record contents
   */
  public ProducerRecord<K, V> orderRecord(String topic, K key, V value) {
    return new ProducerRecord<>(topic, partition, null, key, value, null);
  }

  /**
   * Create a record with no key
   *
   * @param topic The topic this record should be sent to
   * @param value The record contents
   */
  public ProducerRecord<K, V> orderRecord(String topic, V value) {
    return new ProducerRecord<>(topic, partition, null, null, value, null);
  }

  public int partition() {
    return partition;
  }
}
