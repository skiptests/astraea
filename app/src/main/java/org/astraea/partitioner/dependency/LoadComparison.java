package org.astraea.partitioner.dependency;

import static org.astraea.Utils.realHost;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.astraea.Utils;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.astraea.topic.Offset;
import org.astraea.topic.TopicAdmin;

public class LoadComparison {
  private final KafkaProducer<?, ?> producer;
  private final Map<TopicPartition, Offset> offsets;
  private final TopicAdmin topicAdmin;
  private final Set<String> topic;
  private final Properties props;

  LoadComparison(
      KafkaProducer<?, ?> producer,
      Map<TopicPartition, Offset> offsets,
      TopicAdmin topicAdmin,
      Set<String> topic,
      Properties props) {
    this.producer = producer;
    this.offsets = offsets;
    this.topic = topic;
    this.topicAdmin = topicAdmin;
    this.props = props;
  }

  int lessLoad() {
    var minTwoOffset = minTwoOffsets();
    var brokers = new HashMap<Integer, Integer>();
    for (Integer broker : topicAdmin.brokerIds()) {
      topicAdmin.partitionsOfBrokers(topic, Set.of(broker)).stream()
          .map(TopicPartition::partition)
          .filter(minTwoOffset::contains)
          .forEach(partition -> brokers.put(broker, partition));

      if (brokers.size() == 2) {
        break;
      }
    }

    var jmxAddress = jmxAddress(brokers);
    return brokers.get(goodConditionBroker(jmxAddress));
  }

  List<Integer> minTwoOffsets() {
    return offsets.entrySet().stream()
        .sorted(Comparator.comparing(entry -> entry.getValue().latest()))
        .map(entry -> entry.getKey().partition())
        .collect(Collectors.toList())
        .subList(0, 2);
  }

  ProducerMetadata producerMetadata() {
    return (ProducerMetadata) Utils.requireField(producer, "metadata");
  }

  List<JMXAddress> jmxAddress(Map<Integer, Integer> brokers) {
    var cluster = producerMetadata().fetch();
    var bootstrap =
        cluster.nodes().stream()
            .filter(node -> brokers.containsKey(node.id()))
            .collect(Collectors.toMap(Node::id, node -> realHost(node.host())));

    var JMXs = new ArrayList<JMXAddress>();
    bootstrap
        .entrySet()
        .forEach(
            entry -> {
              Utils.jmxAddress(Utils.propsToMap(props)).entrySet().stream()
                  .forEach(
                      hostPort -> {
                        if (hostPort.getKey().equals(entry.getValue())) {
                          JMXs.add(
                              new JMXAddress(
                                  hostPort.getKey(), hostPort.getValue(), entry.getKey()));
                        }
                      });
            });

    return JMXs;
  }

  private Metrics metrics(JMXAddress jmx) {
    return new Metrics(jmx);
  }

  int goodConditionBroker(List<JMXAddress> jmxAddress) {
    var metrics = jmxAddress.stream().map(this::metrics).collect(Collectors.toList());
    var firstBroker = metrics.get(0);
    var secondBroker = metrics.get(metrics.size() - 1);

    var normalized =
        (firstBroker.inPerSec / (firstBroker.inPerSec + secondBroker.inPerSec) - 1) * 0.4
            + (firstBroker.outPerSec / (firstBroker.outPerSec + secondBroker.outPerSec) - 1) * 0.4
            + (firstBroker.jvmMemoryUsage
                        / (firstBroker.jvmMemoryUsage + secondBroker.jvmMemoryUsage)
                    - 1)
                * 0.1
            + (firstBroker.systemCpuUsage
                        / (firstBroker.systemCpuUsage + secondBroker.systemCpuUsage)
                    - 1)
                * 0.1;

    if (normalized > 0) return secondBroker.brokerID;
    else return firstBroker.brokerID;
  }

  static class JMXAddress {
    private final String host;
    private final int port;
    private final int brokerID;

    JMXAddress(String host, int port, int brokerID) {
      this.host = host;
      this.port = port;
      this.brokerID = brokerID;
    }
  }

  private static class Metrics {
    private final double inPerSec;
    private final double outPerSec;
    private final double jvmMemoryUsage;
    private final double systemCpuUsage;
    private final int brokerID;

    private Metrics(JMXAddress jmx) {
      try (var mBeanClient = MBeanClient.jndi(jmx.host, jmx.port)) {
        this.inPerSec = KafkaMetrics.BrokerTopic.BytesInPerSec.fetch(mBeanClient).oneMinuteRate();
        this.outPerSec = KafkaMetrics.BrokerTopic.BytesOutPerSec.fetch(mBeanClient).oneMinuteRate();
        this.jvmMemoryUsage =
            (KafkaMetrics.Host.jvmMemory(mBeanClient).heapMemoryUsage().getUsed() + 0.0)
                / KafkaMetrics.Host.jvmMemory(mBeanClient).heapMemoryUsage().getMax();
        this.systemCpuUsage = KafkaMetrics.Host.operatingSystem(mBeanClient).systemCpuLoad();
        this.brokerID = jmx.brokerID;
      }
    }
  }
}
