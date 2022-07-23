/records
===

- [傳送資料](#傳送資料)
- [讀取資料](#讀取資料)
  - [DISTANCE_FROM_LATEST](#DISTANCE_FROM_LATEST)
  - [DISTANCE_FROM_BEGINNING](#DISTANCE_FROM_BEGINNING)
  - [SEEK_TO](#SEEK_TO)
- [刪除資料](#刪除資料)

## 傳送資料
```shell
POST /records
```

參數

| 名稱            | 說明                                                                                                           | 預設值   |
|---------------|--------------------------------------------------------------------------------------------------------------|-------|
| records       | (必填) 本次寫入資料，需填入至少一筆資料                                                                                        | 無     |
| transactionId | (選填) 填入 transaction id 以執行 transaction 寫入，此值為字串                                                              | 無     |
| async         | (選填) 是否要等到資料寫入 topic 再回傳                                                                                     | false |
| timeout       | (選填) 寫入資料最大時限，逾時便中斷本次操作。格式為 `數值` + `單位`，單位可填寫 `days`, `day`, `h`, `m`, `s`, `ms`, `us`, `ns`。範例：1h，5s，1000ms | 5s    |

- 若 async = true，且操作成功，僅回傳 HTTP 202

records 每筆資料欄位

| 名稱              | 說明                        | 預設值    |
|-----------------|---------------------------|--------|
| topic           | (必填) topic 名稱             | 無      |
| partition       | (選填) 指定要寫入此筆資料的 partition | 無      |
| keySerializer   | (選填) key serializer       | string |
| valueSerializer | (選填) value serializer     | string |
| key             | (選填) key 值                | null   |
| value           | (選填) value 值              | null   |
| timestamp       | (選填) 此筆資料時間戳記             | null   |

- keySerializer/valueSerializer 可選擇 `bytearray`/`strinng`/`long`/`integer`/`float`/`double`
- 若 serializer 選擇 `bytearray`，需將值為 byte array 的 key/value 做 base64 encoding 轉字串

cURL 範例

```shell
curl -X POST http://localhost:8001/records \
    -H "Content-Type: application/json" \
    -d '{
    "async": false,
    "transactionId": "trx-1",
    "records": [
      {
        "topic": "test1",
        "partition": 0,
        "keySerializer": "string",
        "valueSerializer": "bytearray",
        "key": "test",
        "value": "dGVzdA==", #此為 "test" 經 base64 encoding 後之字串
        "timestamp": 1656337829
      },
      {
        "topic": "test1",
        "partition": 0,
        "keySerializer": "string",
        "valueSerializer": "bytearray",
        "key": "test",
        "value": "dGVzdA==", #此為 "test" 經 base64 encoding 後之字串
        "timestamp": 1656337829
      }
    ]}'
```
JSON Response (僅在 async = false 時回傳)

- topic: topic 名稱
- partition: 此筆資料所處 partition
- offset: 此筆資料 offset
- timestamp: 此筆資料時間戳記
- serializedKeySize: key 經序列化後之 byte array 長度
- serializedValueSize: value 經序列化後之 byte array 長度

```json
{
  "results": [
    {
      "topic": "test1",
      "partition": 0,
      "offset": 0,
      "timestamp": 1656337829,
      "serializedKeySize": 4,
      "serializedValueSize": 4
    },
    {
      "topic": "test1",
      "partition": 0,
      "offset": 1,
      "timestamp": 1656337829,
      "serializedKeySize": 4,
      "serializedValueSize": 4
    }
  ]
}
```

## 讀取資料

```shell
GET /records/{topic}
```

參數

| 名稱                    | 說明                                                                                                           | 預設值    |
|-----------------------|--------------------------------------------------------------------------------------------------------------|--------|
| partition             | (選填) 指定要讀取之 partition                                                                                        | 無      |
| keyDeserializer       | (選填) key deserializer                                                                                        | string |
| valueDeserializer     | (選填) value deserializer                                                                                      | string |
| limit                 | (選填) 回傳資料筆數上限                                                                                                | 1      |
| timeout               | (選填) 請求資料最大時限，逾時便中斷本次操作。格式為 `數值` + `單位`，單位可填寫 `days`, `day`, `h`, `m`, `s`, `ms`, `us`, `ns`。範例：1h，5s，1000ms | 5s     |
| distanceFromLatest    | (選填) 距離最新 offset 往前多少位移量開始拉取資料                                                                               | 無      |
| distanceFromBeginning | (選填) 距離起始 offset 往後多少位移量開始拉取資料                                                                               | 無      |
| seekTo                | (選填) 從指定 offset 開始拉取資料                                                                                       | 無      |

- keyDeserializer/valueDeserializer 可選擇 `bytearray`/`string`/`long`/`integer`/`float`/`double`
- 若 deserializer 選擇 `bytearray`，回傳的 key/value 值為經 base64 encoding 後之字串
- distanceFromLatest / distanceFromBeginning / seekTo 僅能從三者中挑選一種使用。若三者都不填寫，預設行為與 consumer `auto.offset.reset=latest` 一致

以下範例均假設已經在僅有一個 partition 之 topic `test` 並插入 10 筆資料，key 型別為 string, value 型別為 integer，key/value 值
從第一筆開始為 `key: "test0", value: 0` 至第十筆為 `key: test9, value: 9`

### DISTANCE_FROM_LATEST

從距離最新的 offset 往前推 3 位移，並拉取兩筆資料。

cURL 範例

```shell
curl -X GET "http://localhost:8001/records/test?distanceFromLatest=3&limit=2&keyDeserializer=string&valueDeserializer=integer"
```

JSON Response

- topic: topic 名稱
- partition: 此筆資料所處 partition
- offset: 此筆資料 offset
- timestamp: 此筆資料時間戳記
- serializedKeySize: key 經序列化後之 byte array 長度
- serializedValueSize: value 經序列化後之 byte array 長度
- headers: 此筆資料之 headers，為 key-value pair 結構
- key: key 值
- value: value 值
- leaderEpoch: 代表 replica leader 發生幾次切換

```json
{
  "data": [
    {
      "topic": "test",
      "partition": 0,
      "offset": 7,
      "timestamp": 1656755990246,
      "serializedKeySize": 5,
      "serializedValueSize": 4,
      "headers": [],
      "key": "test7",
      "value": 7,
      "leaderEpoch": 0
    },
    {
      "topic": "test",
      "partition": 0,
      "offset": 8,
      "timestamp": 1656755990246,
      "serializedKeySize": 5,
      "serializedValueSize": 4,
      "headers": [],
      "key": "test8",
      "value": 8,
      "leaderEpoch": 0
    }
  ]
}
```

### DISTANCE_FROM_BEGINNING

從距離起始 offset 往後推 3 位移，並拉取兩筆資料。

cURL 範例

```shell
curl -X GET "http://localhost:8001/records/test?distanceFromBeginning=3&limit=2&keyDeserializer=string&valueDeserializer=integer"
```

JSON Response

```json
{
  "data": [
    {
      "topic": "test",
      "partition": 0,
      "offset": 3,
      "timestamp": 1656755990246,
      "serializedKeySize": 5,
      "serializedValueSize": 4,
      "headers": [],
      "key": "test3",
      "value": 3,
      "leaderEpoch": 0
    },
    {
      "topic": "test",
      "partition": 0,
      "offset": 4,
      "timestamp": 1656755990246,
      "serializedKeySize": 5,
      "serializedValueSize": 4,
      "headers": [],
      "key": "test4",
      "value": 4,
      "leaderEpoch": 0
    }
  ]
}
```

### SEEK_TO

從指定 offset 開始拉取資料

cURL 範例 - 從 offset=1 開始拉取共兩筆資料

```shell
curl -X GET "http://localhost:8001/records/test?seekTo=1&limit=2&keyDeserializer=string&valueDeserializer=integer"
```

JSON Response

```json
{
  "data": [
    {
      "topic": "test",
      "partition": 0,
      "offset": 1,
      "timestamp": 1656755990246,
      "serializedKeySize": 5,
      "serializedValueSize": 4,
      "headers": [],
      "key": "test1",
      "value": 1,
      "leaderEpoch": 0
    },
    {
      "topic": "test",
      "partition": 0,
      "offset": 2,
      "timestamp": 1656755990246,
      "serializedKeySize": 5,
      "serializedValueSize": 4,
      "headers": [],
      "key": "test2",
      "value": 2,
      "leaderEpoch": 0
    }
  ]
}
```

## 刪除資料
```shell
DELETE /records/{topic}
```

Request 參數

| 名稱        | 說明                            | 預設值           |
|-----------|-------------------------------|---------------|
| offset    | long (選填) 刪除指定offset之前的record | latest offset |
| partition | int (選填) 指定partition          | 所有partition   |

cURL 範例

```shell
curl -X DELETE "http://localhost:8001/records/mytopic?partition=5&offset=2"
```