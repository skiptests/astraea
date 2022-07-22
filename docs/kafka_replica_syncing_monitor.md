### Kafka Replica Syncing Monitor

此工具可以監控follower replicas的同步進度，也可以拿來監控目前partition的搬移進度

#### 使用範例

專案內有將此工具整合到Docker中，方便使用者管理

編譯執行：

```bash
$ ./gradlew run --args="monitor --bootstrap.servers 192.168.103.39:9092"
```

Docker執行：

```bash
$ ./docker/start_app.sh monitor --bootstrap.servers 192.168.103.70:16036
```

#### 輸出

```bash
[2021-11-23T16:00:26.282676667]
  Topic "my-topic":
  | Partition 0:
  | | replica on broker   0 => [####################] 100.00% [leader, synced]
  | | replica on broker   2 => [                    ]   1.37% 0.00 B/s (unknown) []
  | Partition 1:
  | | replica on broker   0 => [####################] 100.00% [leader, synced]
  | | replica on broker   2 => [                    ]   1.35% 0.00 B/s (unknown) []

[2021-11-23T16:00:26.862637796]
  Topic "my-topic":
  | Partition 0:
  | | replica on broker   0 => [####################] 100.00% [leader, synced]
  | | replica on broker   2 => [#                   ]   5.62% 240.54 MB/s (11s estimated) []
  | Partition 1:
  | | replica on broker   0 => [####################] 100.00% [leader, synced]
  | | replica on broker   2 => [#                   ]   5.25% 242.53 MB/s (12s estimated) []

[2021-11-23T16:00:27.400814839]
  Topic "my-topic":
  | Partition 0:
  | | replica on broker   0 => [####################] 100.00% [leader, synced]
  | | replica on broker   2 => [##                  ]   9.90% 242.53 MB/s (10s estimated) []
  | Partition 1:
  | | replica on broker   0 => [####################] 100.00% [leader, synced]
  | | replica on broker   2 => [##                  ]   9.13% 240.54 MB/s (11s estimated) []

...
```

列出目前非同步的partition的同步進度，以及預估完成同步的時間

##### 配合參數啟動

指定topic #`my-topic`，不論有沒有非同步replica都持續監控

```bash
./docker/start_app.sh monitor --bootstrap.servers 192.168.103.70:16036 --topics my-topic --track
```

輸出：

```bash
[2022-07-22T08:18:05.167553]
  Every replica is synced.

[2022-07-22T08:18:06.164502]
  Every replica is synced.

[2022-07-22T08:18:07.163866]
  Every replica is synced.
...
```

改變監控replicas state的頻率

```bash
./docker/start_app.sh monitor --bootstrap.servers 192.168.103.70:16036 --interval 500ms --track
```

輸出：

```bash
[2022-07-22T08:55:58.506569]
  Every replica is synced.

[2022-07-22T08:55:59.006518]
  Every replica is synced.

[2022-07-22T08:55:59.505859]
  Every replica is synced.

[2022-07-22T08:56:00.005014]
  Every replica is synced.
...
```

#### Replica Syncing Monitor Configurations

1. --bootstrap.servers: 欲連接的server
2. --interval: 查看replicas state的頻率，支援浮點數 (default: 1 second)
3. --prop.file: Kafka admin的properties檔案路徑
4. --topic: 選擇要追蹤的topic (default: 追蹤所有 non-synced partition)
5. --track: 若所有replicas已經同步，仍持續監控，並持續監控任何non-synced replicas(if any) (default: false)