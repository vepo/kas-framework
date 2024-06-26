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