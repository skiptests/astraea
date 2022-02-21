package org.astraea.partitioner.smoothPartitioner;

import static org.astraea.Utils.jmxAddress;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.astraea.partitioner.nodeLoadMetric.LoadPoisson;
import org.astraea.partitioner.nodeLoadMetric.NodeLoadClient;

/**
 * Based on the jmx metrics obtained from Kafka, it records the load status of the node over a
 * period of time. Predict the future status of each node through the poisson of the load status.
 * Finally, the result of poisson is used as the weight to perform smooth weighted RoundRobin.
 */
public class SmoothWeightPartitioner implements Partitioner {

  /**
   * Record the current weight of each node according to Poisson calculation and the weight after
   * partitioner calculation.
   */
  private final Map<Integer, int[]> brokersWeight = new HashMap<>();

  private NodeLoadClient nodeLoadClient;

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    Map<Integer, Integer> loadCount;
    var rand = new Random();
    loadCount = loadCount(cluster);
    var loadPoisson = new LoadPoisson();

    Objects.requireNonNull(loadCount, "OverLoadCount should not be null.");
    brokersWeight(loadPoisson.allPoisson(loadCount));
    AtomicReference<Map.Entry<Integer, int[]>> maxWeightServer = new AtomicReference<>();
    var allWeight = allNodesWeight();
    brokersWeight.forEach(
        (nodeID, weight) -> {
          if (maxWeightServer.get() == null || weight[1] > maxWeightServer.get().getValue()[1]) {
            maxWeightServer.set(Map.entry(nodeID, weight));
          }
          brokersWeight.put(nodeID, new int[] {weight[0], weight[1] + weight[0]});
        });
    Objects.requireNonNull(maxWeightServer.get(), "MaxWeightServer should not be null.");
    brokersWeight.put(
        maxWeightServer.get().getKey(),
        new int[] {
          maxWeightServer.get().getValue()[0], maxWeightServer.get().getValue()[1] - allWeight
        });
    brokersWeight
        .keySet()
        .forEach(
            brokerID -> {
              if (memoryWarning(brokerID)) {
                subBrokerWeight(brokerID, 100);
              }
            });

    ArrayList<Integer> partitionList = new ArrayList<>();
    cluster
        .partitionsForNode(maxWeightServer.get().getKey())
        .forEach(partitionInfo -> partitionList.add(partitionInfo.partition()));
    return partitionList.get(rand.nextInt(partitionList.size()));
  }

  @Override
  public void close() {
    nodeLoadClient.close();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    try {
      var mapAddress = jmxAddress(configs);
      Objects.requireNonNull(mapAddress, "You must configure jmx_servers correctly.");

      nodeLoadClient = new NodeLoadClient(mapAddress);
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }

  /** Change the weight of the node according to the current Poisson. */
  // visible for test
  public synchronized void brokersWeight(Map<Integer, Double> poissonMap) {
    poissonMap.forEach(
        (key, value) -> {
          var thoughPutAbility = nodeLoadClient.thoughPutComparison(key);
          if (!brokersWeight.containsKey(key)) {
            brokersWeight.put(key, new int[] {(int) ((1 - value) * 20 * thoughPutAbility), 0});
          } else {
            brokersWeight.put(
                key,
                new int[] {(int) ((1 - value) * 20 * thoughPutAbility), brokersWeight.get(key)[1]});
          }
        });
  }

  private void subBrokerWeight(int brokerID, int subNumber) {
    brokersWeight.put(
        brokerID,
        new int[] {brokersWeight.get(brokerID)[0], brokersWeight.get(brokerID)[1] - subNumber});
  }

  private synchronized int allNodesWeight() {
    return brokersWeight.values().stream().mapToInt(vs -> vs[0]).sum();
  }

  private boolean memoryWarning(int brokerID) {
    return nodeLoadClient.memoryUsage(brokerID) >= 0.8;
  }

  private Map<Integer, Integer> loadCount(Cluster cluster) {
    try {
      return nodeLoadClient.loadSituation(cluster);
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
