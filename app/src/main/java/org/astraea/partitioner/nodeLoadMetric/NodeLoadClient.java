package org.astraea.partitioner.nodeLoadMetric;

import static org.astraea.Utils.realHost;

import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.astraea.Utils;
import org.astraea.metrics.collector.Receiver;
import org.astraea.metrics.java.HasJvmMemory;
import org.astraea.metrics.kafka.KafkaMetrics;

/**
 * this clas is responsible for obtaining jmx metrics from BeanCollector and calculating the
 * overload status of each node through them.
 */
public class NodeLoadClient {

  private final List<Receiver> receiverList;
  private final Map<String, Integer> currentJmxAddresses;
  private static final ReceiverFactory RECEIVER_FACTORY = new ReceiverFactory();
  private long lastTime = -1;
  // visible for testing
  List<Broker> brokers;
  private Map<Integer, Integer> currentLoadNode;
  private int referenceBrokerID;
  private CountDownLatch countDownLatch;
  private boolean notInMethod = true;

  public NodeLoadClient(Map<String, Integer> jmxAddresses) throws IOException {
    this.receiverList = RECEIVER_FACTORY.receiversList(jmxAddresses);
    receiverList.forEach(receiver -> Utils.waitFor(() -> receiver.current().size() > 0));
    currentJmxAddresses = jmxAddresses;
  }

  /**
   * When the interval between getting the load twice exceeds one second, update the load and
   * return, otherwise directly return to the existing load.
   *
   * @param cluster from partitioner
   * @return each node load count in preset time
   */
  public synchronized Map<Integer, Integer> loadSituation(Cluster cluster)
      throws UnknownHostException {
    if (overOneSecond() && notInMethod) {
      notInMethod = false;
      nodesOverLoad(cluster);
    }
    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return currentLoadNode;
  }

  /** @param cluster the cluster from the partitioner */
  private synchronized void nodesOverLoad(Cluster cluster) {
    var nodes = cluster.nodes();
    if (lastTime == -1) {
      countDownLatch = new CountDownLatch(1);
      brokers =
          nodes.stream()
              .map(node -> new Broker(node.id(), node.host(), node.port()))
              .collect(Collectors.toList());
      referenceBrokerID = brokers.stream().findFirst().get().brokerID;
      var nodeIDReceiver = new HashMap<Integer, List<Receiver>>();
      receiverList.forEach(
          receiver -> {
            var brokersID = brokerIDOfReceiver(receiver.host());
            brokersID.forEach(
                brokerID -> {
                  if (nodeIDReceiver.containsKey(brokerID)) {
                    nodeIDReceiver.get(brokerID).add(receiver);
                  } else {
                    nodeIDReceiver.put(brokerID, new ArrayList<>(List.of(receiver)));
                  }
                });
          });
      metricsNameForReceiver(nodeIDReceiver);
    }

    brokersMsgPerSec();

    var totalInput = brokers.stream().map(broker -> broker.input).reduce(0.0, Double::sum);
    var totalOutput = brokers.stream().map(broker -> broker.output).reduce(0.0, Double::sum);

    AtomicReference<Double> totalBrokerSituation = new AtomicReference<>(0.0);
    brokers.forEach(
        broker -> {
          broker.maxThoughPut();
          broker.inputNormalized(totalInput);
          broker.outputNormalized(totalOutput);
          broker.thoughPutComparison(findBroker(referenceBrokerID).maxThoughPut);
          broker.brokerSituation();
          totalBrokerSituation.updateAndGet(v -> v + broker.brokerSituation);
        });

    var totalSituation = totalBrokerSituation.get();

    brokers.forEach(broker -> broker.brokerSituationNormalized(totalSituation));

    var avg = totalSituation / brokers.size();

    // TODO Regarding countermeasures against cluster conditions
    var standardDeviation = standardDeviationImperative(avg);

    brokers.forEach(broker -> broker.load(brokerLoad(broker.brokerSituation, avg)));
    currentLoadNode =
        brokers.stream().collect(Collectors.toMap(broker -> broker.brokerID, Broker::totalLoad));
    lastTime = System.currentTimeMillis();
    countDownLatch.countDown();
    notInMethod = true;
  }

  private void brokersMsgPerSec() {
    brokers.forEach(
        broker -> {
          broker.output =
              Double.parseDouble(
                  broker
                      .metrics
                      .get(KafkaMetrics.BrokerTopic.BytesOutPerSec.name())
                      .current()
                      .get(0)
                      .beanObject()
                      .getAttributes()
                      .get("MeanRate")
                      .toString());
          broker.input =
              Double.parseDouble(
                  broker
                      .metrics
                      .get(KafkaMetrics.BrokerTopic.BytesInPerSec.name())
                      .current()
                      .get(0)
                      .beanObject()
                      .getAttributes()
                      .get("MeanRate")
                      .toString());
        });
  }

  public double standardDeviationImperative(double avgBrokersSituation) {
    var variance = new AtomicReference<>(0.0);
    brokers.forEach(
        broker ->
            variance.updateAndGet(
                v ->
                    v
                        + (broker.brokerSituationNormalized - avgBrokersSituation)
                            * (broker.brokerSituationNormalized - avgBrokersSituation)));
    return Math.sqrt(variance.get() / brokers.size());
  }

  /** @param brokerIDReceiver nodes metrics that exists in topic */
  private void metricsNameForReceiver(HashMap<Integer, List<Receiver>> brokerIDReceiver) {
    brokerIDReceiver.forEach(
        (brokerID, receivers) ->
            receivers.stream()
                .map(
                    receiver ->
                        receiver.current().stream()
                            .map(
                                bean ->
                                    bean.beanObject().getProperties().values().stream()
                                        .findAny()
                                        .get())
                            .findAny()
                            .get())
                .collect(Collectors.toList())
                .forEach(
                    name ->
                        findBroker(brokerID)
                            .metrics
                            .put(
                                name,
                                receivers.stream()
                                    .filter(
                                        mbean ->
                                            mbean.current().stream()
                                                .findAny()
                                                .get()
                                                .beanObject()
                                                .getProperties()
                                                .values()
                                                .stream()
                                                .findAny()
                                                .get()
                                                .equals(name))
                                    .findAny()
                                    .get())));
  }

  private Broker findBroker(int brokerID) {
    return brokers.stream().filter(broker -> broker.brokerID == brokerID).findAny().get();
  }

  private boolean overOneSecond() {
    return lastTime + Duration.ofSeconds(1).toMillis() <= System.currentTimeMillis();
  }

  public void close() {
    RECEIVER_FACTORY.close(currentJmxAddresses);
  }

  private List<Integer> brokerIDOfReceiver(String host) {
    return brokers.stream()
        .filter(broker -> Objects.equals(realHost(broker.host), host))
        .map(broker -> broker.brokerID)
        .collect(Collectors.toList());
  }

  // visible of test
  int brokerLoad(double brokerSituation, double avg) {
    var initialization = 5;
    var loadDeviationRate = (brokerSituation - avg) / avg;
    var load = (int) Math.round(initialization + loadDeviationRate * 10);
    if (load > 10) load = 10;
    else if (load < 0) load = 0;
    return load;
  }

  public double thoughPutComparison(int brokerID) {
    return findBroker(brokerID).thoughPutComparison;
  }

  public double memoryUsage(int brokerID) {
    return findBroker(brokerID).memoryUsage();
  }

  // visible of test
  static class Broker {
    private final int brokerID;
    private final String host;
    private final int port;
    private int count = 0;
    private final Map<Integer, Integer> load = new HashMap<>();
    private final Map<String, Receiver> metrics = new HashMap<>();
    private double input;
    private double output;
    private double maxThoughPut;
    private double brokerSituation;
    private double thoughPutComparison;
    private double inputNormalized;
    private double outputNormalized;
    private double brokerSituationNormalized;
    private static final double inputWeights = 0.5;
    private static final double outputWeights = 0.5;

    // visible of test
    Broker(int brokerID, String host, int port) {
      this.brokerID = brokerID;
      this.host = host;
      this.port = port;
    }

    private int totalLoad() {
      return load.values().stream().reduce(0, Integer::sum);
    }

    private void load(Integer currentLoad) {
      this.load.put(count % 10, currentLoad);
      count++;
    }

    private void brokerSituationNormalized(double totalSituation) {
      this.brokerSituationNormalized = brokerSituation / totalSituation;
    }

    private void inputNormalized(double brokersInput) {
      this.inputNormalized = (input + 1) / (brokersInput + 1);
    }

    private void outputNormalized(double brokersOutput) {
      this.outputNormalized = (output + 1) / (brokersOutput + 1);
    }

    private void brokerSituation() {
      this.brokerSituation =
          (inputNormalized * inputWeights + outputNormalized * outputWeights) / thoughPutComparison;
    }

    private void maxThoughPut() {
      this.maxThoughPut = Math.max(maxThoughPut, input + output);
    }

    private void thoughPutComparison(double referenceThoughPut) {
      if (count >= 20) {
        this.thoughPutComparison =
            (maxThoughPut - referenceThoughPut) / (referenceThoughPut + 1) + 1;
      } else if (count >= 10) {
        this.thoughPutComparison =
            (maxThoughPut - referenceThoughPut) / (referenceThoughPut + 1) / 2 + 1;
      } else if (count >= 0) {
        this.thoughPutComparison = 1;
      }
    }

    private double memoryUsage() {
      if (count < 10) {
        return 0.1;
      } else {
        var jvm = (HasJvmMemory) metrics.get("Memory").current().stream().findAny().get();
        return (jvm.heapMemoryUsage().getUsed() + 0.0) / (jvm.heapMemoryUsage().getMax() + 1);
      }
    }
  }
}
