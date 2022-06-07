# kafka-go

POC to get familiar with [Kafka](https://kafka.apache.org/) fundamentals.

- [producer.go](/producer.go) listens for HTTP requests then sends the request payload to Kafka.
- [consumer.go](/consumer.go) listens for Kafka events and writes the event value to file.

```shell
# start zookeeper + kafka broker
docker compose up -d

# build kafka producer + consumer
_script/build.sh

# create topics
_script/create-topics.sh

# in new terminal tab start producer
./out/producer getting-started.properties

# in new terminal tab start the consumer
./out/consumer getting-started.properties
```

### Testing

Bombard the producer with [Rip](https://github.com/bjarneo/rip).

```shell
# in project root: kafka-go/
git clone https://github.com/bjarneo/rip
cd rip
go install
cd ..
rip --interval=10 --concurrent=10 --post --json=test-payload.json http://localhost:3000/extraction
```

### Official Tut

- [Getting Started with Apache Kafka and Go](https://developer.confluent.io/get-started/go/?_ga=2.255802446.753691761.1654621631-1894550548.1652922238)