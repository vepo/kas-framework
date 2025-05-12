package dev.vepo.maestro.experiment.data.generator;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class DataGenerator {
    private static class Vehicle {
        private String id;
        private double latitude;
        private double longitude;
        private double speed; // in km/h
        private double bearing; // in degrees (0-360)
        private boolean accelerating;

        public Vehicle(String id, double lat, double lon) {
            this.id = id;
            this.latitude = lat;
            this.longitude = lon;
            this.speed = ThreadLocalRandom.current().nextDouble(30, 80);
            this.bearing = ThreadLocalRandom.current().nextDouble(0, 360);
            this.accelerating = ThreadLocalRandom.current().nextBoolean();
        }

        public void updatePosition() {
            // Randomly change acceleration state (10% chance)
            if (ThreadLocalRandom.current().nextDouble() < 0.1) {
                accelerating = !accelerating;
            }

            // Adjust speed based on acceleration state
            if (accelerating) {
                speed = Math.min(speed + ThreadLocalRandom.current().nextDouble(0.1, 0.5), 120);
            } else {
                speed = Math.max(speed - ThreadLocalRandom.current().nextDouble(0.1, 0.5), 5);
            }

            // Randomly adjust bearing slightly (5% chance of significant turn)
            if (ThreadLocalRandom.current().nextDouble() < 0.05) {
                bearing += ThreadLocalRandom.current().nextDouble(-45, 45);
            } else {
                bearing += ThreadLocalRandom.current().nextDouble(-5, 5);
            }

            // Normalize bearing
            bearing = (bearing + 360) % 360;

            // Calculate new position (approximate)
            double distance = speed * 1000 / 3600; // km/h to m/s, assuming 1s interval
            double bearingRad = Math.toRadians(bearing);

            // Earth's radius in meters
            double R = 6371000;

            // Convert to radians
            double latRad = Math.toRadians(latitude);
            double lonRad = Math.toRadians(longitude);

            // Calculate new position
            double newLatRad = Math.asin(Math.sin(latRad) * Math.cos(distance / R) +
                    Math.cos(latRad) * Math.sin(distance / R) * Math.cos(bearingRad));
            double newLonRad = lonRad + Math.atan2(Math.sin(bearingRad) * Math.sin(distance / R) * Math.cos(latRad),
                                                   Math.cos(distance / R) - Math.sin(latRad) * Math.sin(newLatRad));

            // Convert back to degrees
            latitude = Math.toDegrees(newLatRad);
            longitude = Math.toDegrees(newLonRad);
        }

        public String toJSON() {
            return String.format("""
                                 {"id":"%s","latitude":"%s","longitude":"%s","speed":%.2f,"bearing":%.2f,"accelerating":%b,"timestamp":%d}""",
                                 id, df.format(latitude), df.format(longitude),
                                 speed, bearing, accelerating, System.currentTimeMillis());
        }
    }

    private static final String TOPIC = "vehicle-moviment";
    private static final String BOOTSTRAP_SERVERS = "kafka-0:9092,kafka-1:9094,kafka-2:9096";
    private static final int THREADS = Runtime.getRuntime().availableProcessors() * 2;
    private static final int TARGET_RATE_PER_THREAD = 90000 / THREADS;
    private static final int NUM_VEHICLES = 1000;
    private static final int BATCH_SIZE = 25;

    private static final DecimalFormat df = new DecimalFormat("0.000000");

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        var generator = new DataGenerator(THREADS);
        generator.start();
    }

    private final int numThreads;
    private final AtomicBoolean running;
    private final CountDownLatch latch;

    public DataGenerator(int numThreads) {
        this.numThreads = numThreads;
        this.running = new AtomicBoolean(true);
        this.latch = new CountDownLatch(1);

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
                                       .mapToObj(t -> executors.submit(() -> runOnce(t)))
                                       .toList();
            for (var c : completions) {
                c.get();
            }
        }
    }

    private void runOnce(int thread) {
        // Create Kafka producer
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 262144);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        try (var producer = new KafkaProducer<String, String>(props)) {

            // Initialize vehicles
            List<Vehicle> vehicles = new ArrayList<>();
            Random rand = new Random();

            // Start all vehicles in Berlin area
            double berlinLat = 52.5200;
            double berlinLon = 13.4050;

            for (int i = 0; i < NUM_VEHICLES; i++) {
                double lat = berlinLat + (rand.nextDouble() - 0.5) * 0.1;
                double lon = berlinLon + (rand.nextDouble() - 0.5) * 0.1;
                vehicles.add(new Vehicle("V" + (100_000 + i), lat, lon));
            }

            // Rate control
            System.out.printf("Starting producer thread [%d]: %d vehicles, %d msg/sec%n",
                              thread, NUM_VEHICLES, TARGET_RATE_PER_THREAD);

            long messagesInBatch = 0;
            long totalMessagesSent = 0;
            long lastReportTime = System.currentTimeMillis();

            // Main loop
            long loopStart = System.nanoTime();
            while (running.get()) {

                // Select random vehicle
                var vehicle = vehicles.get(rand.nextInt(vehicles.size()));

                // Update its position
                vehicle.updatePosition();

                // Send message
                var record = new ProducerRecord<String, String>(TOPIC, vehicle.id, vehicle.toJSON());

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
