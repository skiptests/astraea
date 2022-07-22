### Kafka Partition Score

此工具會為broker上的partition評分，越高的分數代表負載越重。

#### 使用範例

此工具有整合到 container 中，使用者利用 Docker 運行，可方便管理

可自己編譯並執行：

```bash
./gradlew run --args="score --bootstrap.servers 192.168.103.39:9092"
```

使用 Docker 執行

```bash
./docker/start_app.sh score --bootstrap.servers 192.168.103.70:16036
```

#### 搭配參數執行

##### 隱藏internal topic

若想要忽略 internal topic `_consumer_offsets` ，可以使用

```bash
./docker/start_app.sh score --bootstrap.servers 192.168.103.70:16036 --exclude.internal.topics
```

輸出為

```bash
JMX address: 192.168.103.24:12712

broker: 1004
test0-16: 370.95

The following brokers are balanced: [1001, 1002, 1003, 1006, 1007]
The following partitions are balanced: [demo-2, test-3, test-9, test-15, test0-4, test0-10, test0-22, test0-28]
```

會輸出指定的broker id，以及partition的分數，並列出平衡、不平衡的broker及topic-partitions

##### 隱藏internal topic及balance

```bash
./docker/start_app.sh score --bootstrap.servers 192.168.103.70:16036 --exclude.internal.topics --hide.balanced
```

輸出為

```bash
JMX address: 192.168.103.24:19645

broker: 1004
test0-16: 370.95
```

#### Partition Score Configurations

1. --bootstrap.servers: 欲連接的server
2. --exclude.internal.topics: 若在為topics評分時不想為內部topic評分，如:_consumer_offsets
3. --hide.balanced: 若想隱藏已經平衡的topics、partitions