# Maestro: Kafka Automatic Tunning Experiment

## Objective

Create a Kafka Stream that automatically tune itself.

## 🛠 Environment Setup

1. Start Kafka
   ```bash
   ./scripts/start-kafka
   ```
2. Create Topics
   ```bash
   ./scripts/create-topics
   ```
3. Start Data Generator
   ```bash
   ./scripts/start-data-generator
   ```
4. Inject Data
   ```bash
   mvn -pl experiment/data-generator/ clean compile exec:java -Dexec.mainClass=io.vepo.kafka.stream.datagenerator.InjectData -Dexec.args="-d ./experiment/train-data -t 20 -r 1000 train"
   ```
4. Run the experiment
   ```bash
   mvn clean install && mvn -pl experiment/sample-stream/ exec:java -Dexec.mainClass="io.vepo.kafka.stream.sample.SampleStream"
   ```

### Troubleshooting

#### Clean Environment

```bash
./scripts/clean-environment
```

## Steps

1. Create a Stream that has a growing Records lag
2. Listen Kafka Metrics
3. Tune it!

## Documentation:

1. https://developer.ibm.com/articles/monitoring-apache-kafka-apps/
2. https://docs.confluent.io/platform/current/kafka/monitoring.html#records-lead


Failing

> 2025-09-15 03:02:58.227 [kafka-coordinator-heartbeat-thread | maestro] WARN  o.a.k.c.c.i.ConsumerCoordinator - [Consumer clientId=maestro-8376583f-0091-4b9b-a449-2fc9a483a703-StreamThread-1-consumer, groupId=maestro] consumer poll timeout has expired. This means the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, which typically implies that the poll loop is spending too much time processing messages. You can address this either by increasing max.poll.interval.ms or by reducing the maximum size of batches returned in poll() with max.poll.records.
> 2025-09-15 03:02:58.274 [kafka-coordinator-heartbeat-thread | maestro] WARN  o.a.k.c.c.i.ConsumerCoordinator - [Consumer clientId=maestro-8376583f-0091-4b9b-a449-2fc9a483a703-StreamThread-2-consumer, groupId=maestro] consumer poll timeout has expired. This means the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, which typically implies that the poll loop is spending too much time processing messages. You can address this either by increasing max.poll.interval.ms or by reducing the maximum size of batches returned in poll() with max.poll.records.
