/balancer
===

- [排程搜尋新的負載優化計劃](#排程搜尋新的負載優化計劃)
- [執行負載優化計劃](#執行負載優化計劃)
- [查詢負載優化計劃的狀態](#查詢負載優化計劃的狀態)

## 排程搜尋新的負載優化計劃
```shell
POST /balancer
```

參數

| 名稱             | 說明                                                         | 預設值                                                                                                                                                                                 |
|----------------|------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| clusterCosts   | (必填) 指定要優化的目標以及對應權重                                        | 無                                                                                                                                                                                   |
| timeout        | (選填) 指定產生時間                                                | 3s                                                                                                                                                                                  |
| balancer       | (選填) 欲使用的負載優化計劃搜尋演算法                                       | org.astraea.common.balancer.algorithms.GreedyBalancer                                                                                                                               |
| balancerConfig | (選填) 搜尋演算法的實作細節參數，此為一個 JSON Object 內含一系列的 key/value String | 無                                                                                                                                                                                   |
| moveCosts      | (必填) 指定要計算的各項搬移成本                                          | "org.astraea.common.cost.ReplicaLeaderCost", "org.astraea.common.cost.RecordSizeCost", "org.astraea.common.cost.ReplicaNumberCost", "org.astraea.common.cost.ReplicaLeaderSizeCost" |
| costConfig     | (選填) 針對各個搬移成本做限制，此為一個 JSON Object 內含一系列的 key/value String  | 無                                                                                                                                                                                   |

官方記錄的 balancerConfig:

`balancerConfig` 是 balancer 實作開放給使用者設定的內部演算法行為參數，我們有針對常用情境的 balancer config 規範出一些固定的名稱，
參數是否支援要看 Balancer 實作本身。當指定的參數不被 balancer 實作支援時，該實作可能會丟出錯誤提示使用者。

| config key                     | config value                                                                                                                                                                                                                                                                                               |
|--------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| 
| balancer.allowed.topics.regex  | 一個正則表達式，表達允許進行搬移操作的 topic 名稱，當沒有指定的時候，代表沒有任何限制，所有 topic 都可以做搬移，包含 internal topics，表格下面有附上一些常見的設定範例，可以當做參考。                                                                                                                                                                                                 |
| balancer.broker.balancing.mode | 這個參數指定每個 Broker 要採取的負載平衡設定，目前擁有的模式包含 `balancing`, `clear` 和 `excluded`。`balancing` 代表特定節點要參予負載平衡的過程，該節點身上的負載可以被變動。`clear` 代表特定節點身上的負載必須要全部移除，這個功能能夠協助使用者優雅地下線一個節點。`excluded` 代表特定節點不能夠參予負載平衡的過程，節點不能新增或移除負載。這個參數的格式是一系列的 key/value pair 的字串，每個 pair 之間透過逗號 "," 間隔，而 key/value 之間透過冒號 ":" 間隔，如 `(brokerId_A |"default"):(mode),(brokerId_B):(mode), ...`，其中 `key` 欄位代表這個是描述某 id 節點的設定，而對應的 `value` 欄位則是該節點要套用的負載平衡模式(`balancing`, `clear` 或 `excluded`)，另外 `key` 欄位可以填寫特殊字串 `default`，代表沒有被設定所提及的節點應該使用的負載平衡模式，比如填寫 `default:excluded` 可以使設定沒有提到的節點不參予負載平衡的過程，預設的 `default` 模式是 `balancing`，意即所有設定沒有提到的節點都將參予負載平衡的過程。 |

一些可以參考的 `balancer.allowed.topics.regex` 設定:

| `balancer.allowed.topics.regex` 設定範例          | 效果                                                                                      |
|-----------------------------------------------|-----------------------------------------------------------------------------------------|
| `^(?!__).*$`                                  | 所有非 `__` 開頭的 topic 都可以搬移，如 `__consumer_offsets` 和 `__transaction_state` 之外的 topic 都可以搬移 |
| `^(?!__).*(?<!\.replica)$`                    | 所有非 `__` 開頭或非 `.replica` 結尾的 topic 都可以搬移                                                |
| `^(?!__).*(?<!\.replica)(?<![\-\.]internal)$` | 所有非 `__` 開頭 或非 `.replica`, `.internal`, `-internal` 結尾的 topic 都可以搬移                     |
| `^(topicA\|topicB\|topicC)$`                  | 只有 `topicA`, `topicB` 和 `topicC` 可以搬移                                                   |

costConfig: 

| config key                  | config value                                 | value format                                                                                                                 |
| --------------------------- |----------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| max.migrated.size           | 設定最大可搬移的資料量                                  | "`data size` +  `unit`" ex.100KB, 500MB, 3GB                                                                                 |
| max.migrated.leader.number  | 設定最大可搬移的leader 數量                            | "`limit number`" ex. 1,2,3,100                                                                                               |
| max.migrated.replica.number | 設定最大可搬移的replica 數量                           | "`limit number`" ex. 1,2,3,100                                                                                               |
| max.migrated.leader.size    | 設定最大可搬移的leader 資料量                           | "`data size` +  `unit`" ex.100KB, 500MB, 3GB                                                                                 |
| max.migrated.time.limit     | 設定最大可接受的partition搬移時間                        | "`limit second`" ex. 1,2,3,100                                                                                               |
| max.broker.total.disk.space | 設定搬移過程中broker最大可以佔用的replica 資料量              | "`broker Id` + `:` + `data size` " ex. "0:1500MB ,1:1000MB ,2:1500MB"                                                        |
| max.broker.path.disk.space  | 設定搬移過程中broker上的data folder最大可以佔用的replica 資料量 | "`broker Id`  + `-` + `data path` + `:` + `data size` " ex. "0-/path0:1500MB,1-/path0:1000MB,2-/path0:1500MB,2-/path1:900MB" |

目前支援的 Cost Function

| CostFunction 名稱                              | 優化目標                                             |
|----------------------------------------------|--------------------------------------------------|
| `org.astraea.common.cost.ReplicaLeaderCost`  | 使每個節點服務的 leader partition 數量平均                   |
| `org.astraea.common.cost.ReplicaNumberCost`  | 使每個節點服務的 partition 數量平均                          |
| `org.astraea.common.cost.NetworkIngressCost` | 使每個節點承受的輸入流量接近，此 cost function 使用上有些注意事項，詳情見下方附註 |
| `org.astraea.common.cost.NetworkEgressCost`  | 使每個節點承受的輸出流量接近，此 cost function 使用上有些注意事項，詳情見下方附註 |

* `NetworkIngressCost` 和 `NetworkEgressCost` 的實作存在一些假設，當這些假設不成立時，負載優化的結果可能會出現誤差：
  1. 每個 Partition 的網路輸入/輸出流量是定值，不太會隨時間而波動。
  2. Consumer 總是訂閱和撈取整個 topic 的資料，不會有只對個別 partition 進行撈取的行為。
  3. 當連續針對 `NetworkIngressCost` 或 `NetworkEgressCost` 進行負載優化時
     ，兩個負載優化計劃之間的執行需要有至少 20 分鐘的間隔時間，避免此實作將非生產環境正常行為的效能指標納入考量。
  4. 沒有使用 [Consumer Rack Awareness](https://cwiki.apache.org/confluence/x/go_zBQ)。

cURL 範例
```shell
curl -X POST http://localhost:8001/balancer \
    -H "Content-Type: application/json" \
    -d '{
          "timeout": "5s",
          "balancer": "org.astraea.common.balancer.algorithms.GreedyBalancer",
          "balancerConfig": {
            "shuffle.tweaker.min.step": 1,
            "shuffle.tweaker.max.step": 10
          },
          "clusterCosts": [
            { "cost": "org.astraea.common.cost.ReplicaLeaderCost", "weight": 1 }
          ],
          "moveCosts": [
            "org.astraea.common.cost.ReplicaLeaderCost",
            "org.astraea.common.cost.RecordSizeCost"
          ],
          "costConfig": {
            "max.migrated.size": "500MB",
            "max.migrated.leader.number": 5,
            "max.broker.total.disk.space": "0:1500MB,1:1000MB,2:1500MB",
            "max.broker.path.disk.space": "0-/path0:1500MB,1-/path0:1000MB,2-/path0:1500MB,2-/path1:1000MB"
          }
       }'
```

JSON Response 範例
- `id`: 這個負載優化計劃的編號，後續可以透過這個編號來查詢此計劃的狀態。

```json
{
  "id": "46ecf6e7-aa28-4f72-b1b6-a788056c122a"
}
```

> ##### 搜尋負載優化計劃需要時間
> 透過 `POST /balancer` 發起搜尋後，由於演算法邏輯和叢集效能資訊收集因素，這個計劃可能會花上一段時間才會找到。
> `POST /balancer` 回傳的計劃 id 能夠透過 [GET /balancer/{id}](#查詢負載優化計劃的狀態) 查詢其搜尋狀態，
> 如果其 response 欄位 `generated` 為 `true`，則代表此計劃已經完成搜尋，能夠被 `PUT /balancer` 執行。
> 嘗試執行一個還沒完成搜尋的負載優化計劃會發生錯誤。

## 執行負載優化計劃

```shell
PUT /balancer
```

cURL 範例

```shell
curl -X PUT http://localhost:8001/balancer \
    -H "Content-Type: application/json" \
    -d '{ 
      "id": "46ecf6e7-aa28-4f72-b1b6-a788056c122a",
      "executor": "org.astraea.common.balancer.executor.StraightPlanExecutor",
      "executorConfig": {
        "enableDataDirectoryMigration": false
      }
    }'
```

JSON Request 格式

| 名稱             | 說明                                                           | 預設值                                                       |
|----------------|--------------------------------------------------------------|-----------------------------------------------------------|
| id             | (必填) 欲執行的負載優化計劃之編號                                           | 無                                                         |
| executor       | (選填) 欲使用的計劃執行演算法                                             | org.astraea.common.balancer.executor.StraightPlanExecutor |
| executorConfig | (選填) 計劃執行演算法的實作細節參數，此為一個 JSON Object 內含一系列的 key/value String | 無                                                         |

JSON Response 範例

* `id`: 被接受的負載優化計劃編號。

```json
{ "id": "46ecf6e7-aa28-4f72-b1b6-a788056c122a" }
```

後續能用特定 [API](#查詢負載優化計劃的狀態) 來查詢負載優化計劃的執行進度。

> ##### 進行 Data Directory 負載轉移的風險
> Kafka 本身支援在同一個節點進行 Data Directory 之間的負載轉移，但在 Apache Kafka Version 2.2.0 
> 到 3.5.0 之間存在一個 [Bug](https://issues.apache.org/jira/browse/KAFKA-9087)，這個 Bug 
> 會使這類的負載轉移操作有機率卡住無法結束。 由於這個風險的存在，預設上 `StraightPlanExecutor`
> 的實作會忽略這種同節點的負載搬移行為。這樣做雖然可以避免觸發 Bug，但由於部分計劃內容被忽略沒有套用，
> 最後負載優化的結果可能會和預期的結果失真。這個行為可以透過設定覆蓋，使用者可以透過設定 `executorConfig` 
> 的 `enableDataDirectoryMigration` 設定為 `true` 來啟動 Data Directory 之間的負載轉移。
> 
> 由於這個錯誤是有機率性的觸發，如果你賭一把進行 Data Directory 的轉移，但事後真的不幸觸發錯誤，
> 可以參考[故障排除文件](../troubleshooting.md#Balancer-的-Data-Directory-負載轉移失敗)，
> 那邊有觸發這個 bug 時的一些症狀和解決方案。

> ##### 一個叢集同時間只能執行一個負載優化計劃
> 嘗試對一個叢集同時套用多個負載優化計劃會導致意外的結果，因此 `PUT /balancer` 被設計為：
> 同時間只能夠執行一個負載優化計劃，嘗試執行多個負載優化計劃，那只有一個請求會被接受，其他請求將會被拒絕。
> 注意 Web Service 只能夠避免對當前執行 process 的多個執行請求做有效預防。在執行計劃前 Web Service
> 會檢查是否有正在進行的 Partition Reassignment，如果有偵測到則意味着可能有其他負載優化計劃正在運行。
> Web Service 在這個情況下也會拒絕執行負載優化計劃。

## 查詢負載優化計劃的狀態

```shell
GET /balancer/{id}
```

cURL 範例

```shell
# curl -X GET http://localhost:8001/balancer/{id}
curl -X GET http://localhost:8001/balancer/46ecf6e7-aa28-4f72-b1b6-a788056c122a
```

| 名稱  | 說明                 | 預設值 |
|-----|--------------------|-----|
| id  | (必填) 欲查詢的負載優化計劃之代號 | 無   |

> 目前實作不保留 web service 程式過去啟動時所接受的負載優化計劃進度與結果

> 當查詢的 `id` 沒有對應到任何負載優化計劃，回傳的 HTTP Status Code 會是 `404`

JSON Response 範例

* `id`: 此 Response 所描述的負載優化計劃之編號
* `phase`: 代表此負載優化計劃狀態的字串，可能是下列任一值
  * `Searching`: 正在搜尋能使叢集變更好的負載優化計劃
  * `Searched`: 計劃搜尋已經結束
  * `Executing`: 正在將負載優化計劃套用至叢集
  * `Executed`: 此負載優化計劃已經成功套用至叢集
* `exception`: 當負載優化計劃發生結束時，其所附帶的錯誤訊息。如果沒有錯誤，此欄位會是 `null`，可能觸發錯誤的時間點包含：
  1. 搜尋負載優化計劃的過程中發生錯誤 (此情境下 `phase` 會是 `Searched`)
  2. 執行負載優化計劃的過程中發生錯誤 (此情境下 `phase` 會是 `Executed`)
* `config` 此優化計劃的搜尋參數設定
  * `balancer`: 此計劃生成所使用的搜尋算法實作
  * `function`: 用來評估叢集狀態之品質的方法
  * `timeoutMs`: 此優化計劃的搜尋上限時間
* `plan`: 此負載優化計劃的詳細資訊，如果此計劃還沒完成搜尋，或是已經完成搜尋但找不到可用的計劃，則此欄位會是 `null`
  * `changes`: 新的 partitions 配置
    * `topic`: topic 名稱
    * `partition`: partition id
    * `before`: 原本的配置
      * `brokerId`: 有掌管 replica 的節點 id
      * `size`: replica 在硬碟上的資料大小
    * `after`: 比較好的配置
      * `brokerId`: 有掌管 replica 的節點 id
  * `migrations`: 計算搬移計畫的成本
    * `function`: 用來評估成本的演算法
    * `totalCost`: 各個broker的成本總和
    * `cost`: 針對各個broker計算成本的改變
      * `brokerId`: 有掌管 replica 的節點 id
      * `cost`: 改變的量，負值表示移出，正值表示移入
    * `unit`: 成本的單位

```json
{
  "id": "46ecf6e7-aa28-4f72-b1b6-a788056c122a",
  "phase": "Executed",
  "config": {
    "balancer": "org.astraea.common.balancer.algorithms.GreedyBalancer",
    "function": "WeightCompositeClusterCost[{\"org.astraea.common.cost.NetworkEgressCost@69be333e\" weight 1.0}, {\"org.astraea.common.cost.NetworkIngressCost@6c5ec944\" weight 1.0}]",
    "timeoutMs": 10000
  },
  "plan": {
    "changes": [
      {
        "topic": "example_topic",
        "partition": 0,
        "before": [ { "brokerId": 1006, "size": 2048 } ],
        "after": [ { "brokerId": 1002 } ]
      },
      {
        "topic": "example_topic",
        "partition": 1,
        "before": [ { "brokerId": 1003, "size": 1024 } ],
        "after": [ { "brokerId": 1004 } ]
      }
    ]
  }
}
```

```json5
{
  "id": "46ecf6e7-aa28-4f72-b1b6-a788056c122a",
  "phase": "Searched",
  "exception": "org.apache.kafka.common.KafkaException: Failed to create new KafkaAdminClient",
  "config": { /* ... */ }
}
```

> ##### `before` 和 `after` 陣列中的位置存在特別含義
> `before` 和 `after` 欄位用 JSON 陣列描述一個 topic/partition 預期的 replica 分佈狀況，
> 其中第一個欄位會被解釋成特定 topic/partition 的 preferred leader，且在負載優化執行後，
> 這個 preferred leader 會被內部計劃的執行邏輯變更為當前 partition 的 leader。
>
> 從 JSON 陣列第二位開始預期都是這個 topic/partition 的 follower logs，特別注意目前內部實作
> 不保證這個 follower logs 的順序是否會一致地反映到 Apache Kafka 內部的儲存資料結構內。

> ##### 優化目標的權重設定注意事項
> 權重的分配在優化算法遇到必須做取捨的情況時很重要，設定不適合的權重，可能會讓算法往比較不重要的優化目標做取捨。
> 
> 假設現在有 3 個優化目標和他們各自的權重
> 1. 節點輸入流量 weight 1
> 2. 節點輸出流量 weight 1
> 3. 節點的 Leader 數量 weight 1
> 
> 上述在進行負載優化時，由於 Leader 的優化和網路吞吐量一樣重要，可能會導致為了多兼容這個優化需求而喪失一些優化的機會。
> 如發現 Balancer 生成計劃的 `newScore` 分數沒辦法貼近 0，則其生成的計劃可能在三者之間做了某種程度的取捨。如果不希望
> 這些取捨發生，或願意對某些不重要的優化項目做取捨，可以考慮調低 Leader 數量優化的權重值。

> ##### 解讀 `score` 的注意事項
> 1. `score` 和 `newScore` 之值代表一個叢集分佈接近最佳狀況的程度。
> 2. 不同負載優化計劃之間的 `score` 分數沒有關聯(不能彼此比較)。
> 3. 同一筆優化計劃所產生分數能夠反映叢集的好壞，即 `scoreA` > `scoreB` 有定義，其代表 B 負載分佈比 A 負載分佈在此特定情境下更好。


目前此 endpoint 僅能查詢負載優化計劃是否完成，如想知道更細部的搬移進度，可考慮使用 [Web Service Reassignments API](web_api_reassignments_chinese.md) 查詢。