### Topic Explorer

此工具可以顯示Kafka cluster中所有topics的

* `topic size`：該topic儲存在file system的容量
* `partition size average`：partition平均佔用的容量
* `partition count`：topic有幾個partition
* `replica count`：總replica數量
* `replica location`：replica分佈在哪個broker上
* `consume progress`：consumer group 的消費狀況

#### 使用範例

專案內的工具都有整合到container中，使用者利用docker運行，可方便管理

* 無指定topic，會在console印出Kafka叢集內所有topic的訊息

利用source code執行此tool

```bash
./gradlew run --args="offset --bootstrap.servers 192.168.50.178:19993"
```

利用Docker執行此tool

```bash
./docker/start_app.sh offset --bootstrap.servers 192.168.103.70:16036
```

* 若有指定topic，只會印出該topic的資訊。 假設叢集內有一topic #`demo`

利用Docker執行此tool

```ba
./docker/start_app.sh offset --bootstrap.servers 192.168.103.70:16036 --topics demo
```

##### 輸出說明

以指定topic #`demo`為例

```b
➤ ./start_app.sh offset --bootstrap.servers 192.168.103.70:16036 --topics demo
JMX address: 192.168.103.24:14761
[2022-07-22T06:06:38.515809]
 Topic "demo"
 | Statistics of Topic "demo"
 | | Topic Size: 129.195 MB
 | | Partition Count: 5
 | | Partition Size Average: 25.839 MB
 | | Replica Count: 10
 | | Replica Size Average: 12.919 MB
 | Consumer Groups:
 | | Consumer Group "groupId-1658469949012"
 | |   consume progress of partition #0 [####################] (earliest/current/latest offset 0/24583/24583)
 | |   consume progress of partition #1 [####################] (earliest/current/latest offset 0/25230/25230)
 | |   consume progress of partition #2 [####################] (earliest/current/latest offset 0/25962/25962)
 | |   consume progress of partition #3 [####################] (earliest/current/latest offset 0/23744/23744)
 | |   consume progress of partition #4 [####################] (earliest/current/latest offset 0/23278/23278)
 | |   Members:
 | |     no active member.
 | Partitions/Replicas:
 | | Partition "0" (size: 25.872 MB) (offset range: [0, 24583])
 | |   | replica on broker #1002   (size=12.94 MB) [follower, online] at "/tmp/log-folder-2"
 | |   | replica on broker #1006   (size=12.94 MB) [leader, online] at "/tmp/log-folder-0"
 | | Partition "1" (size: 26.507 MB) (offset range: [0, 25230])
 | |   | replica on broker #1003   (size=13.25 MB) [leader, online] at "/tmp/log-folder-1"
 | |   | replica on broker #1007   (size=13.25 MB) [follower, online] at "/tmp/log-folder-2"
 | | Partition "2" (size: 27.460 MB) (offset range: [0, 25962])
 | |   | replica on broker #1001   (size=13.73 MB) [follower, online] at "/tmp/log-folder-0"
 | |   | replica on broker #1004   (size=13.73 MB) [leader, online] at "/tmp/log-folder-1"
 | | Partition "3" (size: 25.067 MB) (offset range: [0, 23744])
 | |   | replica on broker #1002   (size=12.53 MB) [leader, online] at "/tmp/log-folder-1"
 | |   | replica on broker #1006   (size=12.53 MB) [follower, online] at "/tmp/log-folder-1"
 | | Partition "4" (size: 24.289 MB) (offset range: [0, 23278])
 | |   | replica on broker #1003   (size=12.14 MB) [follower, online] at "/tmp/log-folder-2"
 | |   | replica on broker #1007   (size=12.14 MB) [leader, online] at "/tmp/log-folder-1"
 |_____________________________________________________________________________________

```

* Statistics of Topic "demo"：Topic #`demo`的資訊，有partition的數量以及replica的數量
* Consumer Groups：consumers的消費進度，也可以看到有幾個active members
* Partitions/Replicas：可以看到`leader replica`、`follower replica`在哪個broker上，並指出在哪一個data folder

#### Offset Explorer Configurations

1. --bootstrap.servers: 欲連接的server
2. --topics: 欲查找的topics
3. --admin.props.file: Kafka admin的properties檔案路徑