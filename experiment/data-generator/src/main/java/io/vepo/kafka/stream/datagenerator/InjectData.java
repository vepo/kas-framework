package io.vepo.kafka.stream.datagenerator;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.github.bucket4j.Bucket;
import io.vepo.maestro.experiment.data.TrainInfo;
import io.vepo.maestro.experiment.data.TrainMoviment;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

@Command(name = "inject-data", mixinStandardHelpOptions = true, version = "inject-data 0.1", description = "inject-data made with jbang")
public class InjectData implements Callable<Integer> {

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

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(InjectData.class);

    public static void main(String... args) {
        logger.info("Parameters: size={} {}", args.length, Arrays.toString(args));
        int exitCode = new CommandLine(new InjectData()).execute(args);
        System.exit(exitCode);
    }

    private static List<Path> findAllFiles(Path folder, String extension) {
        var allFiles = new ArrayList<Path>();
        for (var file : folder.toFile().listFiles()) {
            if (file.isDirectory()) {
                allFiles.addAll(findAllFiles(file.toPath(), extension));
            } else if (file.getName().endsWith(extension)) {
                logger.debug("Found file {}", file.toPath());
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
        logger.info("Injecting train moviment data");
        var configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 104857600);
	    configs.put(ProducerConfig.BATCH_SIZE_CONFIG, 10);
	//configs.put(ProducerConfig.LINGER_MS_CONFIG, 5);
	//configs.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 45000);

        var producer = new KafkaProducer<String, String>(configs);
        var objectMapper = new ObjectMapper();
        var executors = Executors.newFixedThreadPool(sendThreads);
        var running = new AtomicBoolean(true);
        var latch = new CountDownLatch(1);

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
                   try {
                       latch.await(10, TimeUnit.SECONDS);
                   } catch (InterruptedException e) {
                       Thread.currentThread().interrupt();
                   }
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
                                 logger.debug("Sending train data {}", moviment);
                                 producer.send(new ProducerRecord<>("train.moviment",
                                                                    null,
                                                                    moviment.publishTimestamp(),
                                                                    moviment.trainId(),
                                                                    objectMapper.writeValueAsString(moviment)),
                                               (metadata, exception) -> {
                                                   if (exception != null) {
                                                       logger.error("Error sending train data", exception);
                                                   } else {
                                                       sentRecords.incrementAndGet();
                                                       partitionThroughput.compute(new RecordCoordinates(metadata.topic(),
                                                                                                         metadata.partition()),
                                                                                   (k, v) -> v == null ? 1 : v + 1);
                                                       logger.debug("Sent train data {}", metadata);
                                                   }
                                               });
                             } catch (InterruptedException ie) {
                                 Thread.currentThread().interrupt();
                             } catch (JsonProcessingException e) {
                                 logger.error("Error serializing flow data", e);
                             } catch (Exception e) {
                                 logger.error("Error sending flow data", e);
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
		    double durationNanos = (double) end - start;
                    long counter = sentRecords.getAndUpdate(v -> 0);
                    double rate = ((double) counter * Duration.ofSeconds(1).toNanos()) / durationNanos;
                    logger.info("Sent {} records at rate {} records/s", counter, rate);
                    partitionThroughput.forEach((k, v) -> logger.info("Throughput {}-{}: {} records/s", k.topic(),
                                                                      k.partition(), ((double) v * Duration.ofSeconds(1).toNanos()) / durationNanos));
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
        latch.countDown();
        executors.shutdown();
        logger.info("Finished injecting train data");
        return CommandLine.ExitCode.OK;
    }

    private void printKeyHistogram() {
        var histogram = new TreeMap<String, AtomicInteger>();
        findTrainData().forEach(t -> histogram.computeIfAbsent(t.trainId(), k -> new AtomicInteger(0))
                                              .incrementAndGet());
        histogram.forEach((k, v) -> logger.info("Key {} has {} records", k, v));
    }

    private static final String RND_STRING;

    static {
        Random random = new SecureRandom();
        RND_STRING = random.ints('0', 'z' + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(1024 * 128) // 0.128MB because max.message.bytes is 1MB
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    private Stream<TrainMoviment> findTrainData() {
        var objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        var trainInfoFiles = findAllFiles(dataDirectory.toPath(), ".json");
        return trainInfoFiles.stream()
                             .flatMap(file -> {
                                 try (var inputStream = Files.newInputStream(file)) {
                                     var trainMoviment = new ArrayList<TrainMoviment>();
                                    //  logger.info("LARGE_DATA: {}", Boolean.valueOf(System.getenv("LARGE_DATA")));
                                     if (Boolean.valueOf(System.getenv("LARGE_DATA"))) {
                                         var data = objectMapper.readValue(inputStream, TrainInfo[].class);
                                         for (var train : data) {
                                            var body = train.body();
                                            var bodyTree = objectMapper.readTree(objectMapper.writeValueAsString(body));
                                            // logger.info("Read data: {}", bodyTree);
                                            ((ObjectNode) bodyTree).put("correction_ind", RND_STRING);
                                            // logger.info("Used data: {}", objectMapper.treeToValue(bodyTree, TrainMoviment.class));
                                            trainMoviment.add(objectMapper.treeToValue(bodyTree, TrainMoviment.class));
                                         }
                                     } else {
                                        var data = objectMapper.readValue(inputStream, TrainInfo[].class);
                                        for (var train : data) {
                                            trainMoviment.add(train.body());
                                        }
                                     }
                                     return trainMoviment.stream();
                                 } catch (Exception e) {
                                     logger.error(String.format("Error reading file %s", file), e);
                                     return null;
                                 }
                             })
                             .filter(Objects::nonNull)
                             .filter(t -> t.actualTimestamp() != null && !t.actualTimestamp().isBlank());
    }
}
