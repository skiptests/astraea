package org.astraea.partitioner.nodeLoadMetric;

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class NodeLoadClientTest extends RequireBrokerCluster {
  private NodeLoadClient nodeLoadClient;

  @BeforeAll
  void setUp() throws IOException {
    var map = new HashMap<String, Integer>();
    map.put(jmxServiceURL().getHost(), jmxServiceURL().getPort());
    nodeLoadClient = new NodeLoadClient(map);
  }

  @AfterAll
  void tearDown() {
    nodeLoadClient.close();
  }

  private Field field(Object object, String fieldName) {
    Field field = null;
    try {
      field = object.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    }
    return field;
  }

  @Test
  void testBrokerLoad() {
    Map<Integer, NodeLoadClient.Broker> brokers = new HashMap<>();
    brokers.put(0, new NodeLoadClient.Broker());
    brokers.put(1, new NodeLoadClient.Broker());
    brokers.put(2, new NodeLoadClient.Broker());
    setBrokers(brokers);
    Assertions.assertEquals(nodeLoadClient.brokerLoad(0.37, 1.0 / 3), 6);
    Assertions.assertEquals(nodeLoadClient.brokerLoad(0.01, 1.0 / 3), 0);
    Assertions.assertEquals(nodeLoadClient.brokerLoad(0.8, 1.0 / 3), 10);
  }

  @Test
  void testStandardDeviationImperative() {
    var testMap = new HashMap<Integer, Double>();
    testMap.put(0, 15.0);
    testMap.put(1, 20.0);
    testMap.put(2, 25.0);
    testMap.put(3, 20.0);
    testMap.put(4, 20.0);

    Map<Integer, NodeLoadClient.Broker> brokers = new HashMap<>();
    brokers.put(0, setSituationNormalized(15.0));
    brokers.put(1, setSituationNormalized(20.0));
    brokers.put(2, setSituationNormalized(25.0));
    brokers.put(3, setSituationNormalized(20.0));
    brokers.put(4, setSituationNormalized(20.0));
    setBrokers(brokers);

    Assertions.assertEquals(nodeLoadClient.standardDeviationImperative(20), 3.1622776601683795);
  }

  @Test
  void testLoadSituation() throws UnknownHostException {
    var bootstrapServers = List.of(bootstrapServers().split(","));
    List<Node> nodes = new ArrayList<>();
    AtomicInteger count = new AtomicInteger(0);
    bootstrapServers.forEach(
        str -> {
          var hostPort = str.split(":");
          nodes.add(new Node(count.get(), hostPort[0], Integer.parseInt(hostPort[1])));
          count.getAndIncrement();
        });

    Cluster cluster = Mockito.mock(Cluster.class);
    when(cluster.nodes()).thenReturn(nodes);
    var load = nodeLoadClient.loadSituation(cluster);
    Assertions.assertEquals(load.get(0), 5);
    Assertions.assertEquals(load.get(1), 5);
    Assertions.assertEquals(load.get(2), 5);
    load = nodeLoadClient.loadSituation(cluster);
    Assertions.assertEquals(load.get(0), 5);
    Assertions.assertEquals(load.get(1), 5);
    Assertions.assertEquals(load.get(2), 5);
    sleep(1);
    load = nodeLoadClient.loadSituation(cluster);
    Assertions.assertEquals(load.get(0), 10);
    Assertions.assertEquals(load.get(1), 10);
    Assertions.assertEquals(load.get(2), 10);
  }

  private NodeLoadClient.Broker setSituationNormalized(double situation) {
    var broker = new NodeLoadClient.Broker();
    var brokerSituation = field(broker, "brokerSituationNormalized");
    try {
      brokerSituation.set(broker, situation);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return broker;
  }

  private void setBrokers(Map<Integer, NodeLoadClient.Broker> brokers) {
    var brokersField = field(nodeLoadClient, "brokers");
    try {
      brokersField.set(nodeLoadClient, brokers);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  private static void sleep(int seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
