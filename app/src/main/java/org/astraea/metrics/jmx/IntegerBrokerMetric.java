package org.astraea.metrics.jmx;

public class IntegerBrokerMetric extends JmxBrokerMetric {

  IntegerBrokerMetric(String jmxObjectName, String attributeName) throws IllegalArgumentException {
    super(jmxObjectName, attributeName);
  }
}
