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
package org.astraea.app.balancer;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.balancer.executor.RebalanceAdmin;
import org.astraea.app.balancer.executor.RebalancePlanExecutor;
import org.astraea.app.balancer.generator.RebalancePlanGenerator;
import org.astraea.app.balancer.metrics.IdentifiedFetcher;
import org.astraea.app.balancer.metrics.MetricSource;
import org.astraea.app.common.Utils;
import org.astraea.app.cost.CostFunction;
import org.astraea.app.cost.HasBrokerCost;
import org.astraea.app.cost.HasPartitionCost;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.partitioner.Configuration;

public class Balancer implements AutoCloseable {

  private final BalancerConfigs balancerConfigs;
  private final List<CostFunction> costFunctions;
  private final RebalancePlanGenerator planGenerator;
  private final RebalancePlanExecutor planExecutor;
  private final Predicate<String> topicFilter;
  private final Admin admin;
  private final MetricSource metricSource;
  private final Map<Object, IdentifiedFetcher> fetcherOwnership;
  private final AtomicBoolean isClosed;
  private final AtomicInteger runCount;

  public Balancer(Configuration configuration) {
    this.balancerConfigs = new BalancerConfigs(configuration);
    this.balancerConfigs.sanityCheck();
    this.costFunctions =
        balancerConfigs.costFunctionClasses().stream()
            .map(x -> BalancerUtils.constructCostFunction(x, configuration))
            .collect(Collectors.toUnmodifiableList());
    this.planGenerator =
        BalancerUtils.constructGenerator(
            balancerConfigs.rebalancePlanGeneratorClass(), configuration);
    this.planExecutor =
        BalancerUtils.constructExecutor(
            balancerConfigs.rebalancePlanExecutorClass(), configuration);
    this.topicFilter =
        (topic) -> {
          if (!balancerConfigs.allowedTopics().isEmpty())
            return balancerConfigs.allowedTopics().contains(topic)
                && !balancerConfigs.ignoredTopics().contains(topic);
          else return !balancerConfigs.ignoredTopics().contains(topic);
        };
    // TODO: add support for security-enabled cluster
    this.admin = Admin.of(balancerConfigs.bootstrapServers());

    this.fetcherOwnership =
        costFunctions.stream()
            .filter(cf -> cf.fetcher().isPresent())
            .collect(
                Collectors.toMap(
                    cf -> cf, cf -> new IdentifiedFetcher(cf.fetcher().orElseThrow())));

    this.metricSource =
        BalancerUtils.constructMetricSource(
            balancerConfigs.metricSourceClass(),
            balancerConfigs.asConfiguration(),
            fetcherOwnership.values());

    this.isClosed = new AtomicBoolean(false);
    this.runCount = new AtomicInteger(0);
  }

  /** Run balancer */
  public void run() {
    // run
    var maxRun = balancerConfigs.balancerRunCount();
    while (!Thread.currentThread().isInterrupted() && runCount.getAndIncrement() < maxRun) {
      boolean shouldDrainMetrics = false;
      // let metric warm up
      // TODO: find a way to show the progress, without pollute the logic
      System.out.println("Warmup metrics");
      var t = progressWatch("Warm Up Metrics", 1, metricSource::warmUpProgress);
      t.start();
      metricSource.awaitMetricReady();
      // TODO: find a way to show the progress, without pollute the logic
      t.interrupt();
      Utils.packException(() -> t.join());
      System.out.println("Metrics warmed");

      try {
        // calculate the score of current cluster
        var clusterInfo = newClusterInfo();
        var clusterMetrics = metricSource.allBeans();
        var currentClusterScore = evaluate(clusterInfo, clusterMetrics);
        // TODO: find a way to show the progress, without pollute the logic
        System.out.println("Run " + planGenerator.getClass().getName());
        var bestProposal = seekingRebalancePlan(clusterInfo, clusterMetrics);
        // TODO: find a way to show the progress, without pollute the logic
        System.out.println(bestProposal);
        if (bestProposal.rebalancePlan().isEmpty()) {
          // TODO: find a way to show the progress, without pollute the logic
          System.out.println("No usable rebalance plan found");
          continue;
        }
        var bestCluster =
            BalancerUtils.mockClusterInfoAllocation(
                clusterInfo, bestProposal.rebalancePlan().get());
        var bestScore = evaluate(bestCluster, clusterMetrics);
        System.out.printf(
            "Current cluster score: %.2f, Proposed cluster score: %.2f%n",
            currentClusterScore, bestScore);
        if (!isPlanExecutionWorth(clusterInfo, bestProposal, currentClusterScore, bestScore)) {
          // TODO: find a way to show the progress, without pollute the logic
          System.out.println("The proposed plan is rejected due to no worth improvement");
          continue;
        }
        // TODO: find a way to show the progress, without pollute the logic
        System.out.println("Run " + planExecutor.getClass().getName());
        shouldDrainMetrics = true;
        executePlan(clusterInfo, bestProposal);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        // drain old metrics, these metrics is probably invalid after the rebalance operation
        // performed.
        if (shouldDrainMetrics) metricSource.drainMetrics();
      }
    }
  }

  /** Retrieve a new {@link ClusterInfo}, with info only related to the permitted topics. */
  private ClusterInfo newClusterInfo() {
    var topics =
        admin.topicNames().stream().filter(topicFilter).collect(Collectors.toUnmodifiableSet());
    return admin.clusterInfo(topics);
  }

  private RebalancePlanProposal seekingRebalancePlan(
      ClusterInfo clusterInfo,
      Map<IdentifiedFetcher, Map<Integer, Collection<HasBeanObject>>> clusterMetrics) {
    var tries = balancerConfigs.rebalancePlanSearchingIteration();
    var counter = new LongAdder();
    // TODO: find a way to show the progress, without pollute the logic
    var thread = progressWatch("Searching for Good Rebalance Plan", tries, counter::doubleValue);
    try {
      thread.start();

      var bestMigrationProposal =
          planGenerator
              .generate(clusterInfo)
              .parallel()
              .limit(tries)
              .peek(ignore -> counter.increment())
              .map(
                  plan -> {
                    if (plan.rebalancePlan().isPresent()) {
                      var allocation = plan.rebalancePlan().get();
                      var mockedCluster =
                          BalancerUtils.mockClusterInfoAllocation(clusterInfo, allocation);
                      var score = evaluate(mockedCluster, clusterMetrics);
                      return Map.entry(score, plan);
                    } else {
                      return Map.entry(1.0, plan);
                    }
                  })
              .min(Map.Entry.comparingByKey());

      // find the target with the highest score, return it
      return bestMigrationProposal
          .map(Map.Entry::getValue)
          .orElseThrow(() -> new NoSuchElementException("No Better Plan Found"));
    } finally {
      thread.interrupt();
      Utils.packException(() -> thread.join());
    }
  }

  private void executePlan(ClusterInfo clusterInfo, RebalancePlanProposal proposal) {
    // prepare context
    var allocation =
        proposal.rebalancePlan().orElseThrow(() -> new NoSuchElementException("No Proposal"));
    try (Admin newAdmin = Admin.of(balancerConfigs.bootstrapServers())) {
      var executorFetcher = this.fetcherOwnership.get(planExecutor);
      var metricSource =
          (Supplier<Map<Integer, Collection<HasBeanObject>>>)
              () ->
                  this.metricSource.metrics(
                      clusterInfo.nodes().stream()
                          .map(NodeInfo::id)
                          .collect(Collectors.toUnmodifiableSet()),
                      executorFetcher);
      var rebalanceAdmin = RebalanceAdmin.of(newAdmin, topicFilter);

      // execute
      planExecutor.run(rebalanceAdmin, allocation);
    }
  }

  // visible for testing
  boolean isPlanExecutionWorth(
      ClusterInfo currentCluster,
      RebalancePlanProposal proposal,
      double currentScore,
      double proposedScore) {
    return currentScore > proposedScore;
  }

  private double evaluate(
      ClusterInfo clusterInfo,
      Map<IdentifiedFetcher, Map<Integer, Collection<HasBeanObject>>> metrics) {
    var scores =
        costFunctions.stream()
            .map(
                cf -> {
                  var fetcher = fetcherOwnership.get(cf);
                  var theMetrics = metrics.get(fetcher);
                  var clusterAndMetrics = ClusterInfo.of(clusterInfo, theMetrics);
                  return Map.entry(cf, this.costFunctionScore(clusterAndMetrics, cf));
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    return aggregateFunction(scores);
  }

  /** the lower, the better. */
  private double aggregateFunction(Map<CostFunction, Double> scores) {
    // use the simple summation result, treat every cost equally.
    return scores.values().stream().mapToDouble(x -> x).sum();
  }

  private double costFunctionScore(ClusterInfo clusterInfo, CostFunction costFunction) {

    if (costFunction instanceof HasBrokerCost) {
      return brokerCostScore(clusterInfo, (HasBrokerCost) costFunction);
    } else if (costFunction instanceof HasPartitionCost) {
      return partitionCostScore(clusterInfo, (HasPartitionCost) clusterInfo);
    } else {
      throw new IllegalArgumentException(
          "Unable to extract score from this cost function: " + costFunction.getClass().getName());
    }
  }

  private <T extends HasBrokerCost> double brokerCostScore(
      ClusterInfo clusterInfo, T costFunction) {
    // TODO: revise the default usage
    return costFunction.brokerCost(clusterInfo).value().values().stream()
        .mapToDouble(x -> x)
        .max()
        .orElseThrow();
  }

  private <T extends HasPartitionCost> double partitionCostScore(
      ClusterInfo clusterInfo, T costFunction) {
    // TODO: support this
    throw new UnsupportedOperationException();
  }

  // TODO: this usage will be removed someday
  @Deprecated
  private static Thread progressWatch(String title, double totalTasks, Supplier<Double> accTasks) {
    AtomicInteger counter = new AtomicInteger();

    Supplier<String> nextProgressBar =
        () -> {
          int blockCount = 20;
          double percentagePerBlock = 1.0 / blockCount;
          double now = accTasks.get();
          double currentProgress = now / totalTasks;
          int fulfilled = Math.min((int) (currentProgress / percentagePerBlock), blockCount);
          int rollingBlock = blockCount - fulfilled >= 1 ? 1 : 0;
          int emptyBlocks = blockCount - rollingBlock - fulfilled;

          String rollingText = "-\\|/";
          String filled = String.join("", Collections.nCopies(fulfilled, "-"));
          String rolling =
              String.join(
                  "",
                  Collections.nCopies(
                      rollingBlock, "" + rollingText.charAt(counter.getAndIncrement() % 4)));
          String empty = String.join("", Collections.nCopies(emptyBlocks, " "));
          return String.format("[%s%s%s] (%.2f/%.2f)", filled, rolling, empty, now, totalTasks);
        };

    Runnable progressWatch =
        () -> {
          while (!Thread.currentThread().isInterrupted()) {
            System.out.println("[" + title + "] " + nextProgressBar.get());
            try {
              TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
              break;
            }
          }
          System.out.println("[" + title + "] " + nextProgressBar.get());
          System.out.println();
        };

    return new Thread(progressWatch);
  }

  @Override
  public void close() {
    // avoid being closed twice
    if (isClosed.getAndSet(true)) return;
    admin.close();
    metricSource.close();
  }
}
