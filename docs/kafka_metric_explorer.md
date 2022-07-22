### Kafka Metric Explorer

此工具可以透過JMX來存取Kafka's MBean metrics，專案內有整合到container中，讓使用者利用docker運行，可方便管理。
在執行此工具前，請確保有Kafka server的jmx address，關於啟動Kafka 可參考 [run_kafka_broker](run_kafka_broker.md)。

#### MBean說明

MBean(Managed Bean)，代表執行在JVM上的資源，例如應用程式。

可以用於收集：效能、資源使用率、問題資訊的統計資訊。

#### 使用範例

利用source code執行此tool

```bash
# 從指定的JMX server獲取所有MBeans
./gradlew run --args="metrics --jmx.server 192.168.50.178:1099"

# 從指定的JMX server獲取物件名稱包含"type=Memory"的MBeans
./gradlew run --args="metrics --jmx.server 192.168.50.178:1099 --property type=Memory"

# 從指定的JMX server獲取屬於"kafka.network" domain name的MBeans，這些MBeans的object name包含兩個屬性"request=Metadata"、"name=LocalTimeMs"
./gradlew run --args="metrics --jmx.server 192.168.50.178:1099 --domain kafka.network --property request=Metadata --property name=LocalTimeMs"

# 從指定的JMX server列出所有MBeans的object name
./gradlew run --args="metrics --jmx.server 192.168.50.178:1099 --view-object-name-list"
```

利用Docker執行此tool

```bash
# 從指定的JMX server獲取所有MBeans
./docker/start_app.sh metrics --jmx.server 192.168.103.68:14697

# 從指定的JMX server獲取物件名稱包含"type=Memory"的MBeans
./docker/start_app.sh metrics --jmx.server 192.168.103.68:14697 --property type=Memory

# 從指定的JMX server獲取屬於"kafka.network" domain name的MBeans，這些MBeans的object name包含兩個屬性"request=Metadata"、"name=LocalTimeMs"
./docker/start_app.sh metrics --jmx.server 192.168.103.68:14697 --domain kafka.network --property request=Metadata --property name=LocalTimeMs

# 從指定的JMX server列出所有MBeans的object name
./docker/start_app.sh metrics --jmx.server 192.168.103.68:14697 --view-object-name-list
```

#### Metric Explorer Configurations

1. --jmx.server: 欲連接的Kafka JMX remote server address
2. --domain: 從指定的domain name詢問MBeans (支援wildcard "*" 跟 "?")。 預設: *
3. --property: 從指定的property中詢問MBeans (支援wildcard "*" 跟 "?")。 可以指定這個參數多次。 預設: []
4. --strict-match: 只顯示與它Object name完全相同的Mbeans。 預設: false
5. --view-object-name-list: 列出MBeans的 domain name & properties的list。 預設: false