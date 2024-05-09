package io.vepo.kafka.stream.datagenerator;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.bucket4j.Bucket;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

@Command(name = "inject-data", mixinStandardHelpOptions = true, version = "inject-data 0.1", description = "inject-data made with jbang")
public class InjectData implements Callable<Integer> {

    record FlowInfo(double lat,
                    @JsonProperty("long") double lon,
                    int flow,
                    int period,
                    int accuracy,
                    String timestamp,
                    @JsonProperty("num_lanes") int numLanes) {
    }

    record FlowData(String key, FlowInfo flow) {
        Flow flowData() {
            return new Flow(key.substring(0, key.lastIndexOf("/")),
                            key.substring(key.lastIndexOf("/") + 1),
                            Instant.from(formatter.parse(flow.timestamp())).toEpochMilli(),
                            flow.lat(),
                            flow.lon(),
                            flow.flow(),
                            flow.period(),
                            flow.accuracy(),
                            flow.numLanes());
        }
    }

    record SpeedInfo(double lat,
                     @JsonProperty("long") double lon,
                     int speed,
                     int accuracy,
                     String timestamp,
                     @JsonProperty("num_lanes") int numLanes) {
    }

    record SpeedData(String key, SpeedInfo speed) {
        Speed speedData() {
            return new Speed(key.substring(0, key.lastIndexOf("/")),
                             key.substring(key.lastIndexOf("/") + 1),
                             Instant.from(formatter.parse(speed.timestamp())).toEpochMilli(),
                             speed.lat(),
                             speed.lon(),
                             speed.speed(),
                             speed.accuracy(),
                             speed.numLanes());
        }
    }

    record Speed(String measurementId,
                 String internalId,
                 long publishTimestamp,
                 double latitude,
                 double longitude,
                 int speed,
                 int accuracy,
                 int numLanes) {
    }

    record Flow(String measurementId,
                String internalId,
                long publishTimestamp,
                double latitude,
                double longitude,
                int flow,
                int period,
                int accuracy,
                int numLanes) {
    }

    record RecordCoordinates(String topic, int partition) implements Comparable<RecordCoordinates> {

        @Override
        public int compareTo(RecordCoordinates other) {
            int topicCompare = topic.compareTo(other.topic);
            if (topicCompare == 0) {
                return Integer.compare(partition, other.partition);
            } else {
                return topicCompare;
            }
        }
    }

    private static class DurationConverter implements ITypeConverter<Duration> {
        @Override
        public Duration convert(String value) {
            Matcher matcher = Pattern.compile("^(\\d+)([smh])$")
                                     .matcher(value);
            if (matcher.matches()) {
                int amount = Integer.parseInt(matcher.group(1));
                switch (matcher.group(2)) {
                    case "s":
                        return Duration.ofSeconds(amount);
                    case "m":
                        return Duration.ofMinutes(amount);
                    case "h":
                        return Duration.ofHours(amount);
                    default:
                        throw new IllegalArgumentException("Invalid duration format");
                }
            } else {
                throw new IllegalArgumentException("Invalid duration format");
            }
        }
    }

    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(InjectData.class);

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")
                                                                        .withZone(ZoneId.systemDefault());

    public static void main(String... args) {
        int exitCode = new CommandLine(new InjectData()).execute(args);
        System.exit(exitCode);
    }

    private static List<Path> findAllFiles(Path folder, String extension) {
        var allFiles = new ArrayList<Path>();
        for (var file : folder.toFile().listFiles()) {
            if (file.isDirectory()) {
                allFiles.addAll(findAllFiles(file.toPath(), extension));
            } else if (file.getName().endsWith(extension)) {
                LOGGER.debug("Found file {}", file.toPath());
                allFiles.add(file.toPath());
            }
        }
        return allFiles;
    }

    @Spec
    CommandSpec spec;

    @Option(names = {
        "-d",
        "--data" }, description = "Data directory", required = true)
    File dataDirectory;

    @Option(names = {
        "-r",
        "--rate" }, description = "Rate of data injection records/second", defaultValue = "100")
    int rate;

    @Option(names = {
        "-b",
        "--bootstrap-servers" }, description = "Kafka bootstrap servers", defaultValue = "kafka-0:9092,kafka-1:9094,kafka-2:9096")
    String bootstrapServers;

    @Option(names = {
        "-t",
        "--threads" }, description = "Number of threads to send data", defaultValue = "10")
    int sendThreads;

    @Option(names = {
        "-f",
        "--frequency" }, description = "Report frequency", defaultValue = "1m", converter = DurationConverter.class)
    Duration reportFrequency;

    @Option(names = {
        "-k",
        "--key-histogram" }, description = "Print key histogram and exit", defaultValue = "false")
    boolean keyHistogram;

    public Integer call() {
        throw new ParameterException(spec.commandLine(), "Missing required subcommand");
    }

    @Command
    int train() {
        if (keyHistogram) {
            printKeyHistogram();
            return CommandLine.ExitCode.OK;
        }
        LOGGER.info("Injecting geolocation data");
        var configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485760);

        var producer = new KafkaProducer<String, String>(configs);
        var objectMapper = new ObjectMapper();
        var executors = Executors.newFixedThreadPool(sendThreads);
        var running = new AtomicBoolean(true);

        // use Bucket4j with greedy strategy to create a constante rate of data
        // injection
        var trainBucket = Bucket.builder()
                                .addLimit(limit -> limit.capacity(rate)
                                                        .refillGreedy(rate, Duration.ofSeconds(1)))
                                .build();

        // add a shutdown hook to stop the application gracefully
        Runtime.getRuntime()
               .addShutdownHook(new Thread(() -> {
                   running.set(false);
                   executors.shutdownNow();
               }));

        AtomicInteger runningThreads = new AtomicInteger(0);
        AtomicLong sentRecords = new AtomicLong(0);
        long start = System.nanoTime();
        var partitionThroughput = Collections.synchronizedMap(new TreeMap<RecordCoordinates, Long>());
        while (running.get()) {
            // start the threads to send data
            // it will start at least 2 threads of each type, but executor will have onlu
            // sendThreads threads
            IntStream.range(0, sendThreads)
                     .forEach(index -> executors.submit(() -> {
                         runningThreads.incrementAndGet();
                         findTrainData().forEach(moviment -> {
                                            try {
                                                trainBucket.asBlocking().consume(1);
                                                LOGGER.debug("Sending flow data {}", moviment);
                                                producer.send(new ProducerRecord<>("train.moviment",
                                                                                   null,
                                                                                   moviment.publishTimestamp(),
                                                                                   moviment.trainId(),
                                                                                   objectMapper.writeValueAsString(moviment)),
                                                              (metadata, exception) -> {
                                                                  if (exception != null) {
                                                                      LOGGER.error("Error sending flow data", exception);
                                                                  } else {
                                                                      sentRecords.incrementAndGet();
                                                                      partitionThroughput.compute(new RecordCoordinates(metadata.topic(),
                                                                                                                        metadata.partition()),
                                                                                                  (k, v) -> v == null ? 1 : v + 1);
                                                                      LOGGER.debug("Sent train data {}", metadata);
                                                                  }
                                                              });
                                            } catch (InterruptedException ie) {
                                                Thread.currentThread().interrupt();
                                            } catch (JsonProcessingException e) {
                                                LOGGER.error("Error serializing flow data", e);
                                            } catch (Exception e) {
                                                LOGGER.error("Error sending flow data", e);
                                            }
                                        });
                         runningThreads.decrementAndGet();
                     }));

            // wait for the threads to finish
            // we should ensure a constant rate of data injection
            // this mean that we have at least 1 thread of each type running
            do {
                if (System.nanoTime() - start > reportFrequency.toNanos()) {
                    long end = System.nanoTime();
                    long counter = sentRecords.getAndUpdate(v -> 0);
                    double rate = ((double) counter * Duration.ofSeconds(1).toNanos()) /
                            ((double) end - start);
                    LOGGER.info("Sent {} records at rate {} records/s", counter, rate);
                    partitionThroughput.forEach((k, v) -> LOGGER.info("Throughput {}-{}: {} records/s", k.topic(),
                                                                      k.partition(), v));
                    partitionThroughput.clear();
                    start = end;
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } while (runningThreads.get() > 2);
        }
        producer.close();
        executors.shutdown();
        LOGGER.info("Finished injecting geolocation data");
        return CommandLine.ExitCode.OK;
    }

    @Command
    int geolocation() {
        if (keyHistogram) {
            printKeyHistogram();
            return CommandLine.ExitCode.OK;
        }
        LOGGER.info("Injecting geolocation data");
        var configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485760);

        var producer = new KafkaProducer<String, String>(configs);
        var objectMapper = new ObjectMapper();
        var executors = Executors.newFixedThreadPool(sendThreads * 2);
        var running = new AtomicBoolean(true);

        // use Bucket4j with greedy strategy to create a constante rate of data
        // injection
        var speedBucket = Bucket.builder()
                                .addLimit(limit -> limit.capacity(rate / 2)
                                                        .refillGreedy(rate / 2, Duration.ofSeconds(1)))
                                .build();
        var flowBucket = Bucket.builder()
                               .addLimit(limit -> limit.capacity(rate / 2)
                                                       .refillGreedy(rate / 2, Duration.ofSeconds(1)))
                               .build();

        // add a shutdown hook to stop the application gracefully
        Runtime.getRuntime()
               .addShutdownHook(new Thread(() -> {
                   running.set(false);
                   executors.shutdownNow();
               }));

        AtomicInteger runningThreads = new AtomicInteger(0);
        AtomicLong sentRecords = new AtomicLong(0);
        long start = System.nanoTime();
        var partitionThroughput = Collections.synchronizedMap(new TreeMap<RecordCoordinates, Long>());
        while (running.get()) {
            // start the threads to send data
            // it will start at least 2 threads of each type, but executor will have onlu
            // sendThreads threads
            IntStream.range(0, sendThreads)
                     .forEach(index -> executors.submit(() -> {
                         runningThreads.incrementAndGet();
                         findFlowData().stream()
                                       .sequential()
                                       .sorted(Comparator.comparing(f -> f.flow()
                                                                          .timestamp(),
                                                                    String.CASE_INSENSITIVE_ORDER))
                                       .forEach(flowData -> {
                                           try {
                                               flowBucket.asBlocking().consume(1);
                                               LOGGER.debug("Sending flow data {}", flowData.flowData());
                                               producer.send(new ProducerRecord<>("flow",
                                                                                  null,
                                                                                  flowData.flowData()
                                                                                          .publishTimestamp(),
                                                                                  flowData.key(),
                                                                                  objectMapper.writeValueAsString(flowData.flowData())),
                                                             (metadata, exception) -> {
                                                                 if (exception != null) {
                                                                     LOGGER.error("Error sending flow data", exception);
                                                                 } else {
                                                                     sentRecords.incrementAndGet();
                                                                     partitionThroughput.compute(new RecordCoordinates(metadata.topic(),
                                                                                                                       metadata.partition()),
                                                                                                 (k, v) -> v == null ? 1 : v + 1);
                                                                     LOGGER.debug("Sent flow data {}", metadata);
                                                                 }
                                                             });
                                           } catch (InterruptedException ie) {
                                               Thread.currentThread().interrupt();
                                           } catch (JsonProcessingException e) {
                                               LOGGER.error("Error serializing flow data", e);
                                           } catch (Exception e) {
                                               LOGGER.error("Error sending flow data", e);
                                           }
                                       });
                         runningThreads.decrementAndGet();
                     }));
            IntStream.range(0, sendThreads)
                     .forEach(index -> executors.submit(() -> {
                         runningThreads.incrementAndGet();
                         findSpeedData().stream()
                                        .sequential()
                                        .sorted(Comparator.comparing(s -> s.speed()
                                                                           .timestamp(),
                                                                     String.CASE_INSENSITIVE_ORDER))
                                        .forEach(speedData -> {
                                            try {
                                                speedBucket.asBlocking().consume(1);
                                                LOGGER.debug("Sending speed data {}", speedData.speedData());
                                                producer.send(new ProducerRecord<>("speed",
                                                                                   null,
                                                                                   speedData.speedData()
                                                                                            .publishTimestamp(),
                                                                                   speedData.key(),
                                                                                   objectMapper.writeValueAsString(speedData.speedData())),
                                                              (metadata, exception) -> {
                                                                  if (exception != null) {
                                                                      LOGGER.error("Error sending speed data",
                                                                                   exception);
                                                                  } else {
                                                                      sentRecords.incrementAndGet();
                                                                      partitionThroughput.compute(new RecordCoordinates(metadata.topic(),
                                                                                                                        metadata.partition()),
                                                                                                  (k, v) -> v == null ? 1 : v + 1);
                                                                      LOGGER.debug("Sent speed data {}", metadata);
                                                                  }
                                                              });
                                            } catch (InterruptedException ie) {
                                                Thread.currentThread().interrupt();
                                            } catch (JsonProcessingException e) {
                                                LOGGER.error("Error serializing speed data", e);
                                            } catch (Exception e) {
                                                LOGGER.error("Error sending speed data", e);
                                            }
                                        });
                         runningThreads.decrementAndGet();
                     }));

            // wait for the threads to finish
            // we should ensure a constant rate of data injection
            // this mean that we have at least 1 thread of each type running
            do {
                if (System.nanoTime() - start > reportFrequency.toNanos()) {
                    long end = System.nanoTime();
                    long counter = sentRecords.getAndUpdate(v -> 0);
                    double rate = ((double) counter * Duration.ofSeconds(1).toNanos()) / ((double) end - start);
                    LOGGER.info("Sent {} records at rate {} records/s", counter, rate);
                    partitionThroughput.forEach((k, v) -> LOGGER.info("Throughput {}-{}: {} records/s", k.topic(),
                                                                      k.partition(), v));
                    partitionThroughput.clear();
                    start = end;
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } while (runningThreads.get() > 2);
        }
        producer.close();
        executors.shutdown();
        LOGGER.info("Finished injecting geolocation data");
        return CommandLine.ExitCode.OK;
    }

    private void printKeyHistogram() {
        var speedData = findSpeedData();
        var flowData = findFlowData();
        var histogram = new TreeMap<String, AtomicInteger>();
        speedData.stream()
                 .map(s -> s.key.substring(0, s.key.lastIndexOf("/")))
                 .forEach(key -> histogram.computeIfAbsent(key, k -> new AtomicInteger(0))
                                          .incrementAndGet());
        flowData.stream()
                .map(s -> s.key.substring(0, s.key.lastIndexOf("/")))
                .forEach(key -> histogram.computeIfAbsent(key, k -> new AtomicInteger(0))
                                         .incrementAndGet());
        histogram.forEach((k, v) -> LOGGER.info("Key {} has {} records", k, v));
    }

    private Stream<TrainMoviment> findTrainData() {
        var objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        var trainInfoFiles = findAllFiles(dataDirectory.toPath(), ".json");
        return trainInfoFiles.stream()
                             .flatMap(file -> {
                                 try (var inputStream = Files.newInputStream(file)) {
                                     var trainMoviment = new ArrayList<TrainMoviment>();
                                     var data = objectMapper.readValue(inputStream, TrainInfo[].class);
                                     for (var train : data) {
                                         trainMoviment.add(train.body());
                                     }
                                     return trainMoviment.stream();
                                 } catch (Exception e) {
                                     LOGGER.error(String.format("Error reading file %s", file), e);
                                     return null;
                                 }
                             })
                             .filter(Objects::nonNull)
                             .filter(t -> t.actualTimestamp() != null && !t.actualTimestamp().isBlank());
    }

    private List<SpeedData> findSpeedData() {
        var objectMapper = new ObjectMapper();
        var geolocations = findAllFiles(dataDirectory.toPath(), ".txt");
        return geolocations.stream()
                           .flatMap(file -> {
                               try {
                                   var speedData = new ArrayList<SpeedData>();
                                   for (var line : Files.readAllLines(file)) {
                                       var parts = line.split("=");
                                       if (parts.length == 2 && parts[1].contains("\"speed\"")) {
                                           speedData.add(new SpeedData(parts[0],
                                                                       objectMapper.readValue(parts[1],
                                                                                              SpeedInfo.class)));
                                       }
                                   }
                                   return speedData.stream();
                               } catch (Exception e) {
                                   e.printStackTrace();
                                   return null;
                               }
                           }).toList();
    }

    private List<FlowData> findFlowData() {
        var objectMapper = new ObjectMapper();
        var geolocations = findAllFiles(dataDirectory.toPath(), ".txt");
        return geolocations.stream()
                           .flatMap(file -> {
                               try {
                                   var flowData = new ArrayList<FlowData>();
                                   for (var line : Files.readAllLines(file)) {
                                       var parts = line.split("=");
                                       if (parts.length == 2 && parts[1].contains("\"flow\"")) {
                                           flowData.add(new FlowData(parts[0],
                                                                     objectMapper.readValue(parts[1], FlowInfo.class)));
                                       }
                                   }
                                   return flowData.stream();
                               } catch (Exception e) {
                                   e.printStackTrace();
                                   return null;
                               }
                           }).toList();
    }
}
