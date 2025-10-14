package dev.vepo.maestro.experiment.data.generator;

import java.io.File;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(DataGenerator.class);

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

    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        var generator = new DataGenerator(THREADS);
        try(var executor = Executors.newSingleThreadExecutor()) {
            var server = new CommandServer(command -> {
                switch (command) {
                    case START:
                        executor.submit(() -> generator.start());
                        break;
                    case STOP:
                        generator.stop();
                        break;
                    case DONE:
                        generator.stop(); // Also stop on DONE command
                        break;
                }
            });
            server.start(7777);
        }
    }

    private final int numThreads;
    private final AtomicBoolean running;
    private final CountDownLatch latch;
    private final Runner runner;
    private ThreadPoolExecutor executorService;
    private final AtomicBoolean isStarted;

    public DataGenerator(int numThreads) {
        this.numThreads = numThreads;
        this.running = new AtomicBoolean(false);
        this.latch = new CountDownLatch(1);
        this.isStarted = new AtomicBoolean(false);
        this.runner = switch (System.getenv("DATA")) {
            case "RAW" -> this::runOnceRaw;
            case "STATS" -> this::runOnceStats;
            default -> throw new InvalidParameterException("Invalid DATA value! %s".formatted(System.getenv("DATA")));
        };
    }

    public void start() {
        if (isStarted.getAndSet(true)) {
            logger.warn("DataGenerator is already started");
            return;
        }
        
        running.set(true);
        
        Runtime.getRuntime()
               .addShutdownHook(new Thread(() -> {
                   logger.info("Shutdown hook triggered");
                   this.stop();
               }));
        
        executorService = new ThreadPoolExecutor(numThreads,
                                                numThreads,
                                                0L, TimeUnit.MILLISECONDS,
                                                new LinkedBlockingQueue<>(10000),
                                                new ThreadPoolExecutor.CallerRunsPolicy());
        
        try {
            var completions = IntStream.range(0, numThreads)
                                       .mapToObj(t -> executorService.submit(() -> runner.run(t)))
                                       .toList();
            
            // Wait for all tasks to complete (which happens when running becomes false)
            for (var c : completions) {
                c.get();
            }
            
            logger.info("All producer threads completed");
            latch.countDown();
            
        } catch (InterruptedException e) {
            logger.error("Data generation interrupted", e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Error during data generation", e);
        } finally {
            isStarted.set(false);
        }
    }

    public void stop() {
        if (!running.getAndSet(false)) {
            logger.info("DataGenerator is already stopped");
            return;
        }
        
        logger.info("Stopping DataGenerator...");
        
        // Shutdown executor service
        if (executorService != null) {
            try {
                executorService.shutdown();
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.warn("Forcing executor service shutdown");
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                logger.warn("Interrupted during executor shutdown", e);
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        // Count down the latch to release any waiting threads
        latch.countDown();
        
        logger.info("DataGenerator stopped successfully");
    }

    public boolean isRunning() {
        return running.get() && isStarted.get();
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
            logger.info("Starting STATS producer thread {}: {} msg/sec", thread, TARGET_RATE_PER_THREAD);

            long messagesInBatch = 0;
            long totalMessagesSent = 0;
            long lastReportTime = System.currentTimeMillis();

            // Main loop
            long loopStart = System.nanoTime();
            while (running.get() && !Thread.currentThread().isInterrupted()) {
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
                    logger.info("Thread[{}]: Sent {} messages ({} msg/sec)",
                                      thread, totalMessagesSent, totalMessagesSent / ((System.currentTimeMillis() - lastReportTime) / 1000.0));
                    totalMessagesSent = 0;
                    lastReportTime = System.currentTimeMillis();
                }
            }

            logger.info("Producer thread [{}] completed", thread);
        } catch (InterruptedException e) {
            logger.info("Producer thread [{}] interrupted", thread);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Error in producer thread [{}]", thread, e);
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
            logger.info("Starting RAW producer thread [{}]: {} msg/sec", thread, TARGET_RATE_PER_THREAD);

            long messagesInBatch = 0;
            long totalMessagesSent = 0;
            long lastReportTime = System.currentTimeMillis();

            // Main loop
            long loopStart = System.nanoTime();
            while (running.get() && !Thread.currentThread().isInterrupted()) {
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
                    logger.info("Thread[{}]: Sent {} messages ({} msg/sec)",
                                      thread, totalMessagesSent, totalMessagesSent / ((System.currentTimeMillis() - lastReportTime) / 1000.0));
                    totalMessagesSent = 0;
                    lastReportTime = System.currentTimeMillis();
                }
            }

            logger.info("Producer thread [{}] completed", thread);
        } catch (InterruptedException e) {
            logger.info("Producer thread [{}] interrupted", thread);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Error in producer thread [{}]", thread, e);
        }
    }
}