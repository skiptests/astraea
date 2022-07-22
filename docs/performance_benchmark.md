### Performance Benchmark

此工具可產生各種不同類型的資料集來測試讀寫速度、E2E的延遲。資料集包含：

1. 大量小型資料寫入
2. 大量大資料寫入
3. 混合型資料寫入
4. 具交易行為的資料寫入

可量測的數據如下：

1. Publish latency : 完成producer request的時間
2. End-to-End latency : 一筆record從producer端到consumer端的時間
3. Consume rate : consumer拉取資料的速率(MB/s)
4. Produce rate : producer送資料的速率(MB/s)

#### Performance Benchmark Configurations

|       參數名稱       |                             說明                             |              預設值              |
| :------------------: | :----------------------------------------------------------: | :------------------------------: |
| --bootstrap.servers  |             (必填) 欲連接的Kafka server address              |                無                |
|    --compression     | (選填) Kafka Producer使用的壓縮演算法，可用的壓縮演算法為：`gzip`, `snappy`, `lz4`, `zstd` |               none               |
|       --topic        |                    (選填) 選擇topic name                     | testPerformance-{Time in millis} |
|     --partitions     |            (選填) 建立topic時，設定的partition數             |                1                 |
|      --replicas      |             (選填) 建立topic時，設定的replica數              |                1                 |
|     --consumers      |            (選填) 欲開啟的consumer thread(s)數量             |                1                 |
|     --producers      |            (選填) 欲開啟的producer thread(s)數量             |                1                 |
|     --run.until      | (選填) producers要送的records數或是producers在給定時間內一直發送資料，格式為`數值`+`單位`<br />若是選擇producers要送多少records，範例："--run.until 89000records"<br />若選擇producer在給定時間內發送資料，時間單位可以選擇`days`, `day`, `h`, `m`, `s`, `ms`, `us`, `ns`，範例："--run.until 1m"。 |           1000records            |
|      --key.size      |               (選填) 每筆record key的大小上限                |              4Byte               |
|  --key.distribution  | (選填) key和key大小的分佈名稱，可用的分佈為：`uniform`, `zipfian`, `latest`, `fixed` |              No Key              |
|     --value.size     |              (選填) 每筆record value的大小上限               |               1KiB               |
| --value.distribution | (選填) value和value大小的分佈名稱， 可用的分佈為: `uniform`, `zipfian`, `latest`, `fixed` |             uniform              |
|     --prop.file      |                (選填) 配置property file的路徑                |               none               |
|    --partitioner     |             (選填) 配置producer使用的partitioner             |               none               |
|      --configs       | (選填) 給partitioner的設置檔。 設置格式為 "<key1>=<value1>[,<key2>=<value2>]*"。 <br />例如: "--configs broker.1001.jmx.port=14338,org.astraea.cost.ThroughputCost=1" |               none               |
|     --throughput     | (選填) 所有producers的produce rate, 範例： "--throughput 2MiB" |           500 GiB/sec            |
|   --specify.broker   |         (選填) 指定broker的ID，送資料到指定的broker          |               none               |
|    --report.path     |                 (選填) report file的檔案路徑                 |               none               |
|   --report.format    |      (選填) 選擇輸出檔案格式, 可用的格式：`csv`, `json`      |               csv                |
|  --transaction.size  | (選填) 每個transaction的records數量。若設置1以上，會使用transaction，否則都是一般write |                1                 |

#### 使用範例

專案內的工具都有整合到container中，使用者利用docker運行，可方便管理

使用前，請先確認自己的Kafka server ip，並且Kafka 有正常運作，關於啟動Kafka 可參考 [run_kafka_broker](run_kafka_broker.md)。

可以使用 docker 執行

```bash 
docker/start_app.sh performance --bootstrap.servers localhost:9092
```

(localhost, 9092 替換成自己Kafka server 的 ip 和 port)

![performance_tool_demo](pictures/performance_tool_demo.jpg)

或者自己重新編譯、執行

```bash
./gradlew run --args="performance --bootstrap.servers localhost:9092"
```

(localhost, 9092 替換成自己Kafka server 的 ip 和 port)

可以指定各種參數如資料大小、分佈、執行時間... 等等。全部參數可以參考上述。

以下列僅列出幾個範例

```bash
# 開啟 1 個 producer 打 25 分鐘資料， 1 個 consumer 消費資料
docker/start_app.sh performance --bootstrap.servers localhost:9092 --run.until 25m
```

```bash
# 開啟 1 個 producer ，打 10000 筆資料 且 沒有consumer
docker/start_app.sh performance --bootstrap.servers localhost:9092 --run.until 10000records --consumers 0
```

```bash
# 打50秒資料、每筆大小10KiB、固定大小、使用4個producer threads、10個consumer threads，指定topic名稱，且該 topic 有 60 partitions，producer送資料前使用 lz4 壓縮演算法
docker/start_app.sh performance --bootstrap.servers localhost:9092 --value.size 10KiB --value.distribution fixed --run.until 50s --producers 4 --consumers 10 --partitions 60 --topic partition60Replica1 --compression lz4
```

```bash
# 使用astraea的 partitioner ，傳入config檔案路徑，裡面可以放 partitioner 所需的參數，如jmx port等
docker/start_app.sh performance --bootstrap.servers localhost:9092 --partitioner org.astraea.app.partitioner.smooth.SmoothWeightRoundRobinDispatcher --prop.file ./config
```

```bash
# 使用 partitioner 框架，指定參考 Broker Input 做效能指標，把紀錄輸出到指定路徑。
docker/start_app.sh performance --bootstrap.servers localhost:9092 --partitioner org.astraea.app.partitioner.StrictCostDispatcher --configs org.astraea.app.cost.BrokerInputCost=1 --prop.file ./config --report.path ~/report
```