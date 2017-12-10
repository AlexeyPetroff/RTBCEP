# Flink CEP usage in RTB

Flink CEP library was used to detect click frauds and low CTR


## Requirements
Install following:

[Apache Flink 1.3](https://ci.apache.org/projects/flink/flink-docs-release-1.3/)

[Apache Kafka 1.0.0 Scala 2.12](https://kafka.apache.org/downloads)

[Apache zookeeper](http://zookeeper.apache.org/releases.html)

## Usage

Run Apache Flink:

```/opt/flink-1.3.2/bin/start-local.sh```

Start Kafka and create topic:
```
sudo /opt/kafka_2.12-1.0.0/bin/zookeeper-server-start.sh /opt/kafka_2.12-1.0.0/config/zookeeper.properties
sudo /opt/kafka_2.12-1.0.0/bin/kafka-server-start.sh /opt/kafka_2.12-1.0.0/config/server.properties
/opt/kafka_2.12-1.0.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 200 --topic CEP
```

Run CEPMonitoring with following parameters:
```
--topic CEP --bootstrap.servers localhost:9092 --group.id SBG --out file:///CEPAlarms/ -Xmx12048m
```

Run Kafka producer:

```
./producer/icu.py CEP

```

The result will be stored in `/CEPRTB/` or in any folder given in `out` parameter