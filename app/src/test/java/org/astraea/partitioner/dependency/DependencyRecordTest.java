package org.astraea.partitioner.dependency;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.consumer.Consumer;
import org.astraea.consumer.Deserializer;
import org.astraea.consumer.Header;
import org.astraea.partitioner.smoothPartitioner.SmoothWeightPartitioner;
import org.astraea.producer.Producer;
import org.astraea.producer.Serializer;
import org.astraea.service.RequireBrokerCluster;
import org.astraea.topic.TopicAdmin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DependencyRecordTest extends RequireBrokerCluster {
  private final String brokerList = bootstrapServers();
  private final String topicName = "address";
  TopicAdmin admin = TopicAdmin.of(bootstrapServers());

  private Properties initProConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "id1");
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SmoothWeightPartitioner.class.getName());
    props.put("producerID", 1);
    props.put("jmx_servers", jmxServiceURL().getHost() + ":" + jmxServiceURL().getPort());
    return props;
  }

  @Test
  void testDependencyRecord() {
    admin.creator().topic(topicName).numberOfPartitions(10).create();
    var key = "tainan";
    var timestamp = System.currentTimeMillis() + 10;
    var header = Header.of("a", "b".getBytes());
    try (var producer =
        Producer.builder()
            .keySerializer(Serializer.STRING)
            .configs(
                initProConfig().entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)))
            .build()) {
      DependencyRecord<String, byte[]> dependencyRecord1 =
          new DependencyRecord<>(producer.kafkaProducer(), initProConfig(), topicName);
      try {
        producer
            .sender()
            .topic(topicName)
            .key(key)
            .timestamp(timestamp)
            .headers(List.of(header))
            .partition(dependencyRecord1.partition())
            .run()
            .toCompletableFuture()
            .get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }

      try (var consumer =
          Consumer.builder()
              .brokers(bootstrapServers())
              .fromBeginning()
              .topics(Set.of(topicName))
              .keyDeserializer(Deserializer.STRING)
              .build()) {
        var records = consumer.poll(Duration.ofSeconds(20));
        var record = records.iterator().next();
        assertEquals(topicName, record.topic());
        assertEquals("tainan", record.key());
        assertEquals(1, record.headers().size());
        var actualHeader = record.headers().iterator().next();
        assertEquals(header.key(), actualHeader.key());
        Assertions.assertArrayEquals(header.value(), actualHeader.value());
      }

      DependencyRecord<String, byte[]> dependencyRecord2 =
          new DependencyRecord<>(producer.kafkaProducer(), initProConfig(), topicName);

      Assertions.assertNotEquals(dependencyRecord1.partition(), dependencyRecord2.partition());

      try {
        producer
            .sender()
            .topic(topicName)
            .key(key)
            .timestamp(timestamp)
            .headers(List.of(header))
            .partition(dependencyRecord2.partition())
            .run()
            .toCompletableFuture()
            .get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }

      DependencyRecord<String, byte[]> dependencyRecord3 =
          new DependencyRecord<>(producer.kafkaProducer(), initProConfig(), topicName);
      Assertions.assertNotEquals(dependencyRecord1.partition(), dependencyRecord3.partition());
      Assertions.assertNotEquals(dependencyRecord2.partition(), dependencyRecord3.partition());

      var i = 0;
      while (i < 299) {
        var metaData =
            producer.kafkaProducer().send(dependencyRecord3.orderRecord(topicName, new byte[128]));
        var keyMetaData =
            producer
                .kafkaProducer()
                .send(dependencyRecord3.orderRecord(topicName, key, new byte[128]));
        assertEquals(topicName, metaData.get().topic());
        assertEquals(topicName, keyMetaData.get().topic());
        i++;
      }
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }

    try (var consumer =
        Consumer.builder()
            .brokers(bootstrapServers())
            .fromBeginning()
            .topics(Set.of(topicName))
            .keyDeserializer(Deserializer.STRING)
            .build()) {
      var records = consumer.poll(Duration.ofSeconds(20));
      var recordsCount = records.size();
      while (recordsCount < 600) {
        recordsCount += consumer.poll(Duration.ofSeconds(20)).size();
      }
      assertEquals(600, recordsCount);
    }
  }
}
