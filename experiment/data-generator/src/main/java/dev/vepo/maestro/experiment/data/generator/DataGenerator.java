package dev.vepo.maestro.experiment.data.generator;

import java.io.File;
import java.security.InvalidParameterException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class DataGenerator {
    private static final String NYC_TAXI_TOPIC = "nyc-taxi-trips";
    private static final String RAW_TOPIC = "raw-input";
    private static final String BOOTSTRAP_SERVERS = "kafka-0:9092,kafka-1:9094,kafka-2:9096";
    private static final int THREADS = Runtime.getRuntime().availableProcessors() * 2;
    private static final int TARGET_RATE_PER_THREAD = 100000 / THREADS;
    private static final int BATCH_SIZE = 25;

    @FunctionalInterface
    private interface Runner {
        void run(int threadId);
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        var generator = new DataGenerator(THREADS);
        generator.start();
    }

    private final int numThreads;
    private final AtomicBoolean running;
    private final CountDownLatch latch;
    private final Runner runner;

    public DataGenerator(int numThreads) {
        this.numThreads = numThreads;
        this.running = new AtomicBoolean(true);
        this.latch = new CountDownLatch(1);
        this.runner = switch (System.getenv("DATA")) {
            case "RAW" -> this::runOnceRaw;
            case "STATS" -> this::runOnceStats;
            default -> throw new InvalidParameterException("Invalid DATA value! %s".formatted(System.getenv("DATA")));
        };
    }

    private void start() throws InterruptedException, ExecutionException {
        Runtime.getRuntime()
               .addShutdownHook(new Thread(() -> {
                   this.running.set(false);
                   try {
                       this.latch.await(5, TimeUnit.SECONDS);
                   } catch (InterruptedException e) {
                       System.out.println("System does not exist by itself.");
                   }
               }));
        try (var executors = new ThreadPoolExecutor(numThreads,
                                                    numThreads,
                                                    0L, TimeUnit.MILLISECONDS,
                                                    new LinkedBlockingQueue<>(10000),
                                                    new ThreadPoolExecutor.CallerRunsPolicy())) {
            var completions = IntStream.range(0, numThreads)
                                       .mapToObj(t -> executors.submit(() -> runner.run(t)))
                                       .toList();
            for (var c : completions) {
                c.get();
            }
        }
    }

    private void runOnceStats(int thread) {
        // Create Kafka producer
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 262144);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        var dataSupplier = new DataSupplier(new File("/dataset/data.json"));

        try (var producer = new KafkaProducer<String, String>(props)) {
            // Rate control
            System.out.printf("Starting STATS producer thread [%d]: %d msg/sec%n", thread, TARGET_RATE_PER_THREAD);

            long messagesInBatch = 0;
            long totalMessagesSent = 0;
            long lastReportTime = System.currentTimeMillis();

            // Main loop
            long loopStart = System.nanoTime();
            while (running.get()) {
                var data = dataSupplier.next();
                // Send message
                var record = new ProducerRecord<String, String>(NYC_TAXI_TOPIC, null, data.dropTimestamp(), UUID.randomUUID().toString(), data.toJson());

                producer.send(record);
                messagesInBatch++;
                totalMessagesSent++;

                if (messagesInBatch >= BATCH_SIZE) {
                    long now = System.nanoTime();
                    long expectedTime = TimeUnit.SECONDS.toNanos(BATCH_SIZE) / TARGET_RATE_PER_THREAD;
                    long actualTime = now - loopStart;

                    if (actualTime < expectedTime) {
                        long sleepTime = expectedTime - actualTime;
                        TimeUnit.NANOSECONDS.sleep(sleepTime);
                    }

                    messagesInBatch = 0;
                    loopStart = System.nanoTime();
                }

                // Periodic reporting (every second)
                if (System.currentTimeMillis() - lastReportTime >= 5000) {
                    System.out.printf("Thread[%d]: Sent %d messages (%.1f msg/sec)%n",
                                      thread, totalMessagesSent, totalMessagesSent / ((System.currentTimeMillis() - lastReportTime) / 1000.0));
                    totalMessagesSent = 0;
                    lastReportTime = System.currentTimeMillis();
                }
            }

            System.out.println("Producer completed");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void runOnceRaw(int thread) {
        // Create Kafka producer
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 262144);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        var dataSupplier = new RawDataSupplier();

        try (var producer = new KafkaProducer<byte[], byte[]>(props)) {
            // Rate control
            System.out.printf("Starting RAW producer thread [%d]: %d msg/sec%n", thread, TARGET_RATE_PER_THREAD);

            long messagesInBatch = 0;
            long totalMessagesSent = 0;
            long lastReportTime = System.currentTimeMillis();

            // Main loop
            long loopStart = System.nanoTime();
            while (running.get()) {
                var data = dataSupplier.next();
                // Send message
                var record = new ProducerRecord<byte[], byte[]>(RAW_TOPIC, UUID.randomUUID().toString().getBytes(), data);

                producer.send(record);
                messagesInBatch++;
                totalMessagesSent++;

                if (messagesInBatch >= BATCH_SIZE) {
                    long now = System.nanoTime();
                    long expectedTime = TimeUnit.SECONDS.toNanos(BATCH_SIZE) / TARGET_RATE_PER_THREAD;
                    long actualTime = now - loopStart;

                    if (actualTime < expectedTime) {
                        long sleepTime = expectedTime - actualTime;
                        TimeUnit.NANOSECONDS.sleep(sleepTime);
                    }

                    messagesInBatch = 0;
                    loopStart = System.nanoTime();
                }

                // Periodic reporting (every second)
                if (System.currentTimeMillis() - lastReportTime >= 5000) {
                    System.out.printf("Thread[%d]: Sent %d messages (%.1f msg/sec)%n",
                                      thread, totalMessagesSent, totalMessagesSent / ((System.currentTimeMillis() - lastReportTime) / 1000.0));
                    totalMessagesSent = 0;
                    lastReportTime = System.currentTimeMillis();
                }
            }

            System.out.println("Producer completed");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
