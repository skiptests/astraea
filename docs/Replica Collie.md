### Replica Collie

此工具提供搬移broker的replica到其餘broker、資料夾的功能

#### 將broker_0和broker_1的所有replicas搬移到其他brokers

```bash
./gradlew run --args="replica --bootstrap.servers 192.168.50.178:19993 --from 0,1"
```

#### 搬移broker_0中topic "abc"的所有replicas到broker_1

```bash
./gradlew run --args="replica --bootstrap.servers 192.168.50.178:19993 --from 0 --to 1 --topics abc"
```

#### 搬移broker_0中topic "abc"的replicas到其他資料夾

```bash
./gradlew run --args="replica --bootstrap.servers 192.168.50.178:19993 --from 0 --to 0 --topics abc"
```

#### 搬移broker_0中topic "abc" 跟 "def"中指定的relicas到broker_1

```bash
./gradlew run --args="replica --bootstrap.servers 192.168.50.178:19993 --from 0 --to 1 --topic abc,def --partitions 0,1"
```

#### 搬移broker_0中topic "abc" 跟 "def"的指定replicas到broker_1的指定資料夾中

```bash
./gradlew run --args="replica --bootstrap.servers 192.168.50.178:19993 --from 0 --to 1 --topic abc,def --partitions 0,1 --path /tmp/log1"
```

#### Replica Collie設置

1. --bootstrap.servers: 欲連接的server
2. --from: 指定要從哪個(些)broker(s)中搬移
3. --prop.file: Kafka admin的properties檔案路徑
4. --to: 搬移的目的地
5. --topics: 要搬移的topics名稱
6. --partitions : 要搬移的partitions
7. --path: 搬移replicas的目的地