package dev.vepo.maestro.experiment.stream;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.math3.util.Pair;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.vepo.kafka.maestro.MaestroConfigs;
import dev.vepo.kafka.maestro.MaestroStreams;
import dev.vepo.kafka.maestro.Streams;
import dev.vepo.kafka.maestro.VanillaStreams;
import dev.vepo.kafka.maestro.adapter.rules.AdapterRule;
import dev.vepo.kafka.maestro.adapter.rules.AdjustConsumerFetchSizeRule;
import dev.vepo.kafka.maestro.adapter.rules.BatchProducerRule;
import dev.vepo.kafka.maestro.adapter.rules.ThreadAllocationRule;
import dev.vepo.kafka.maestro.adapter.rules.UseCompressionOnProducerRule;
import dev.vepo.kafka.maestro.metrics.PerformanceMetricsCollector;
import dev.vepo.maestro.experiment.stream.CommandClient.Command;
import dev.vepo.maestro.experiment.stream.StreamPerformanceEvaluation.TopologyDefinition;
import dev.vepo.maestro.experiment.stream.model.FareStats;
import dev.vepo.maestro.experiment.stream.model.PassengerStats;
import dev.vepo.maestro.experiment.stream.model.TaxiTrip;
import dev.vepo.maestro.experiment.stream.model.TipStats;
import dev.vepo.maestro.experiment.stream.model.TripStats;
import dev.vepo.maestro.experiment.stream.serdes.JsonSerde;

public class StreamPerformanceEvaluation implements Runnable {
    public enum Type {
        VANILLA, MAESTRO
    };

    public enum TopologyDefinition {
        STATS, STATS_MORE_OUTPUT, PASSTHROUGH
    }

    private static final Logger logger = LoggerFactory.getLogger(StreamPerformanceEvaluation.class);
    private static final int TEST_DURATION_IN_MINUTES = 2;

    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        Thread.sleep(5000);
        var client = new CommandClient();
        client.startConnection("producer", 7777);
        var topics = new String[] { "raw-output",
                                    "raw-input",
                                    "raw-by-hash-repartition",
                                    "nyc-taxi-dashboard-fare",
                                    "nyc-taxi-dashboard-tips",
                                    "nyc-taxi-dashboard-passengers",
                                    "nyc-taxi-trips",
                                    "nyc-taxi-trips-by-pu-location-id-repartition",
                                    "nyc-taxi-trips-by-do-location-id-repartition",
                                    "trips-per-hour",
                                    "trips-per-hour-repartition",
                                    "%app-id%-raw-by-hash-repartition",
                                    "%app-id%-raw-unique-store-changelog",
                                    "%app-id%-nyc-taxi-stats-store-changelog",
                                    "%app-id%-nyc-taxi-trips-by-pu-location-id-repartition",
                                    "%app-id%-nyc-taxi-trips-by-do-location-id-repartition",
                                    "%app-id%-trips-per-hour-repartition" };
        Stream.of("warmup", "baseline")
              .forEachOrdered(test -> {
                    try {
                        createTopics(test, topics);
                        client.sendCommand(Command.START);
                        var evaluation = new StreamPerformanceEvaluation(Type.VANILLA, test, TopologyDefinition.STATS);
                        evaluation.run();
                        client.sendCommand(Command.STOP);
                    } catch (Exception e) {
                        logger.error("Error runnint test!", e);
                        System.exit(-1);
                    }
                });
        BiConsumer<String, List<Class<? extends AdapterRule>>> executer = (id, rules) -> {
            try {
                createTopics(id, topics);
                client.sendCommand(Command.START);
                var evaluation = new StreamPerformanceEvaluation(Type.MAESTRO, id, TopologyDefinition.STATS, rules);
                evaluation.run();
                client.sendCommand(Command.STOP);
            } catch (Exception e) {
                logger.error("Error runnint test!", e);
                System.exit(-1);
            }
        };
        executer.accept("all-stats", List.of(ThreadAllocationRule.class,
                                               AdjustConsumerFetchSizeRule.class,
                                               UseCompressionOnProducerRule.class,
                                               BatchProducerRule.class));
        executer.accept("tread-allocation", List.of(ThreadAllocationRule.class));
        client.sendCommand(Command.DONE);
    }

    private static void createTopics(String appId, String... topics) throws InterruptedException, ExecutionException {
        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0:9092,kafka-1:9094,kafka-2:9096");

        try (var adminClient = AdminClient.create(props)) {
            var listTopicsResult = adminClient.listTopics();
            var existingTopics = listTopicsResult.listings().get();

            // Extract topic names (excluding internal topics if desired)
            var existingTopicsNames = existingTopics.stream()
                                                    .map(TopicListing::name)
                                                    // Uncomment the next line to exclude internal topics
                                                    // .filter(name -> !name.startsWith("_"))
                                                    .collect(Collectors.toList());

            if (!existingTopicsNames.isEmpty()) {

                logger.info("Deleting topics: {}", existingTopicsNames);

                // Delete all topics
                DeleteTopicsResult deleteResult = adminClient.deleteTopics(existingTopicsNames);
                deleteResult.all().get(); // Wait for deletion to complete

                logger.info("Successfully deleted {} topics.", existingTopicsNames.size());
            }
            // Create topics
            var result = adminClient.createTopics(Stream.of(topics)
                                                        .map(t -> t.replace("%app-id%", appId))
                                                        .map(topic -> new NewTopic(topic, 12, (short) 2))
                                                        .toList());

            // Wait for completion and get results
            result.all().get();
            logger.info("All topics created successfully");
        }
    }

    private final Type type;
    private final String testId;
    private final TopologyDefinition topology;
    private final List<Class<? extends AdapterRule>> adapterRules;

    public StreamPerformanceEvaluation(Type type, String testId, TopologyDefinition topology) {
        this(type, testId, topology, null);
    }

    public StreamPerformanceEvaluation(Type type, String testId, TopologyDefinition topology, List<Class<? extends AdapterRule>> adapterRules) {
        this.type = type;
        this.testId = testId;
        this.topology = topology;
        this.adapterRules = adapterRules;
        System.setProperty("STATS_FOLDER", Paths.get(System.getenv("STATS_FOLDER"), testId).toAbsolutePath().toString());
    }

    @Override
    public void run() {
        logger.info("Waiting lag grow.... 1 minutes");
        try {
            Thread.sleep(Duration.ofMinutes(1).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        logger.info("Starting Streamer");

        var stateDir = Paths.get("/opt", testId, "state");
        cleanUpFolder(stateDir.toFile());
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0:9092,kafka-1:9094,kafka-2:9096");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, testId);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/opt/" + testId + "/state");
        if (Objects.nonNull(adapterRules)) {
            props.put(MaestroConfigs.ADAPTER_RULE_CLASSES_CONFIG, adapterRules);
        }
        props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, PerformanceMetricsCollector.class.getName());
        try (var maestroStream = create(this::buildTopology, props);
                var taskExecutor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "Main"))) {
            var countDown = new CountDownLatch(1);

            taskExecutor.schedule(() -> {
                maestroStream.close();
                countDown.countDown();
            }, TEST_DURATION_IN_MINUTES, TimeUnit.MINUTES);
            maestroStream.start();
            try {
                countDown.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            logger.info("Streamer stopped");
            taskExecutor.shutdown();
        }
        cleanUpFolder(stateDir.toFile());
    }

    private static void cleanUpFolder(File folder) {
        File[] files = folder.listFiles();
        if (files != null) { // some JVMs return null for empty dirs
            for (File f : files) {
                if (f.isDirectory()) {
                    cleanUpFolder(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }

    private Streams create(Supplier<Topology> topologyProvider, Properties props) {
        return switch (this.type) {
            case VANILLA -> new VanillaStreams(topologyProvider, props);
            case MAESTRO -> new MaestroStreams(topologyProvider, props);
            default -> throw new IllegalStateException("Invalid type: " + this.type);
        };
    }

    public class TripStatsAggregator implements Processor<Integer, TaxiTrip, Integer, TripStats> {
        private WindowStore<Integer, TripStats> store;
        private final Duration duration;
        private ProcessorContext<Integer, TripStats> context;

        public TripStatsAggregator() {
            this.duration = Duration.ofMinutes(1);
        }

        @Override
        public void init(ProcessorContext<Integer, TripStats> context) {
            this.context = context;
            store = context.getStateStore(Topics.NYC_TAXI_STATS_STORE.topicName());
        }

        @Override
        public void process(Record<Integer, TaxiTrip> value) {
            long windowStart = value.timestamp() - (value.timestamp() % duration.toMillis());
            try(var stats = store.fetch(value.key(), windowStart, windowStart)) {
                var currentStats = stats.hasNext() ? stats.next().value : TripStats.initializer();
                var updatedStats = currentStats.add(value.value());
                // Store with window timestamp
                store.put(value.key(), updatedStats, windowStart);
                context.forward(value.withValue(updatedStats));
            }
        }
    }

    private Topology buildTopology() {
        return switch(topology) {
            case STATS -> buildStatsTopology();
            case STATS_MORE_OUTPUT -> buildStatsMoreOutputTopology();
            case PASSTHROUGH -> buildPassthroughTopology();
        };
    }

    private byte[] resizeTo64x64(byte[] inputImage) {
        // Determine if input is square and find dimension
        int inputSize = (int) Math.sqrt(inputImage.length);

        // If not a perfect square, create padded version
        if (inputSize * inputSize != inputImage.length) {
            return padAndResizeTo64x64(inputImage);
        }

        // Input is square, now resize to 64x64
        return resizeSquareImage(inputImage, inputSize, 64);
    }

    private byte[] padAndResizeTo64x64(byte[] inputImage) {
        // Find the smallest square that can contain the image
        int requiredSize = (int) Math.ceil(Math.sqrt(inputImage.length));

        // Create padded square image
        byte[] paddedImage = new byte[requiredSize * requiredSize];

        // Copy original data and pad with zeros
        for (int i = 0; i < inputImage.length; i++) {
            paddedImage[i] = inputImage[i];
        }
        // Remaining bytes are already 0 (default byte value)

        // Now resize the padded square to 64x64
        return resizeSquareImage(paddedImage, requiredSize, 64);
    }

    private byte[] resizeSquareImage(byte[] source, int sourceSize, int targetSize) {
        if (sourceSize == targetSize) {
            return source.clone(); // No resizing needed
        }

        byte[] target = new byte[targetSize * targetSize];

        // Simple nearest-neighbor resizing
        for (int y = 0; y < targetSize; y++) {
            for (int x = 0; x < targetSize; x++) {
                int sourceX = (x * sourceSize) / targetSize;
                int sourceY = (y * sourceSize) / targetSize;
                int sourceIndex = sourceY * sourceSize + sourceX;
                int targetIndex = y * targetSize + x;

                if (sourceIndex < source.length) {
                    target[targetIndex] = source[sourceIndex];
                }
                // Else remains 0 (default)
            }
        }

        return target;
    }

    private byte[] createEmpty64x64Image() {
       return new byte[64 * 64]; // All zeros
    }

    private String md5Hash(byte[] data) {
        try {
            var md = MessageDigest.getInstance("MD5");
            md.update(data);
            return Base64.getEncoder().encodeToString(md.digest());
        } catch (NoSuchAlgorithmException ex) {
            throw new StreamsException("Cannot find MD5 digest. Maybe a missing library.", ex);
        }
    }

    private class UniqueRawProcessor implements Processor<String, byte[], String, byte[]> {
        private ProcessorContext<String, byte[]> context;
        private KeyValueStore<String, Boolean> store;

        @Override
        public void init(ProcessorContext<String, byte[]> context) {
            this.context = context;
            store = context.getStateStore(Topics.RAW_UNIQUE_STORE.topicName());
        }
        @Override
        public void process(Record<String, byte[]> record) {
            if(store.get(record.key()) == null) {
                store.put(record.key(), true);
            } else {
                context.forward(record);
            }
        }
    }

    private Topology buildPassthroughTopology() {
        var builder = new StreamsBuilder();
        var statsStoreBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(Topics.RAW_UNIQUE_STORE.topicName()),
                                                          Serdes.String(),
                                                          Serdes.Boolean())
                                      .withCachingEnabled();
        builder.addStateStore(statsStoreBuilder);
        builder.stream(Topics.RAW_DATA_INPUT.topicName(), Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
               .mapValues(input -> {
                     if (input == null) {
                        return createEmpty64x64Image();
                    }

                    try {
                        return resizeTo64x64(input);
                    } catch (Exception e) {
                        // Log error and return empty image
                        System.err.println("Error resizing image: " + e.getMessage());
                        return createEmpty64x64Image();
                    }
                })
               .selectKey((key, value) -> md5Hash(value))
            //    .repartition(Repartitioned.with(Serdes.String(), Serdes.ByteArray()).withName(Topics.RAW_BY_HASH.topicName()))
            //    .process(UniqueRawProcessor::new, Topics.RAW_UNIQUE_STORE.topicName())
               .to(Topics.RAW_DATA_OUTPUT.topicName(), Produced.with(Serdes.String(), Serdes.ByteArray()));
        return builder.build();
    }

    private Topology buildStatsTopology() {
        // Use TumblingWindows with longer grace period for counting
        var windowSize = Duration.ofMinutes(1);
        var gracePeriod = Duration.ofMinutes(5); // Increased grace period
        var retentionPeriod = windowSize.plus(gracePeriod).plus(Duration.ofMinutes(15)); // Clean up old data
        var builder = new StreamsBuilder();
        var statsStoreBuilder = Stores.windowStoreBuilder(Stores.persistentWindowStore(Topics.NYC_TAXI_STATS_STORE.topicName(),
                                                                                       retentionPeriod,
                                                                                       windowSize,
                                                                      false),
                                                          Serdes.Integer(),
                                                          JsonSerde.of(TripStats.class))
                                      .withCachingEnabled();
        builder.addStateStore(statsStoreBuilder);
        var taxiDataStream = builder.stream(Topics.NYC_TAXI_TRIPS.topicName(), Consumed.with(Serdes.String(), JsonSerde.of(TaxiTrip.class))
                                    .withTimestampExtractor((record, partitionTime) -> {
                                        if (record.value() instanceof TaxiTrip tt) {
                                            return tt.dropTimestamp();
                                        } else {
                                            logger.warn("Wrong input value! record={}", record);
                                            return partitionTime;
                                        }
                                    }));
        var formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
        var zone = ZoneId.of("America/New_York");
        taxiDataStream.selectKey((key, value) -> value.puLocationID())
                      .repartition(Repartitioned.with(Serdes.Integer(), JsonSerde.of(TaxiTrip.class)).withName(Topics.NYC_TAXI_TRIPS_BY_PU_LOCATION_ID.topicName()))
                      .process(TripStatsAggregator::new, Topics.NYC_TAXI_STATS_STORE.topicName())
                      .selectKey((key, value) -> String.format("pu-%d-%s", key, formatter.format(Instant.ofEpochMilli(value.windowStart()).atZone(zone))))
                      .flatMapValues(stats -> List.of(stats.toFare(), stats.toTip(), stats.toPassangers()))
                      .split()
                      .branch((key, stats) -> stats instanceof FareStats, Branched.withConsumer(fareStatsStream -> fareStatsStream.mapValues(stats -> (FareStats) stats)
                                                                                                                                  .to(Topics.NYC_TAXI_DASHBOARD_FARE.topicName(), Produced.with(Serdes.String(), JsonSerde.of(FareStats.class)))))
                      .branch((key, stats) -> stats instanceof TipStats, Branched.withConsumer(tipStatsStreams -> tipStatsStreams.mapValues(stats -> (TipStats) stats)
                                                                                                                                 .to(Topics.NYC_TAXI_DASHBOARD_TIPS.topicName(), Produced.with(Serdes.String(), JsonSerde.of(TipStats.class)))))
                      .noDefaultBranch();
        return builder.build();
    }

    private Topology buildStatsMoreOutputTopology() {
        // Use TumblingWindows with longer grace period for counting
        var windowSize = Duration.ofMinutes(1);
        var gracePeriod = Duration.ofMinutes(5); // Increased grace period
        var retentionPeriod = windowSize.plus(gracePeriod).plus(Duration.ofMinutes(15)); // Clean up old data
        var builder = new StreamsBuilder();
        var statsStoreBuilder = Stores.windowStoreBuilder(Stores.persistentWindowStore(Topics.NYC_TAXI_STATS_STORE.topicName(),
                                                                                       retentionPeriod,
                                                                                       windowSize,
                                                                      false),
                                                          Serdes.Integer(),
                                                          JsonSerde.of(TripStats.class))
                                      .withCachingEnabled();
        builder.addStateStore(statsStoreBuilder);
        var taxiDataStream = builder.stream(Topics.NYC_TAXI_TRIPS.topicName(), Consumed.with(Serdes.String(), JsonSerde.of(TaxiTrip.class))
                                    .withTimestampExtractor((record, partitionTime) -> {
                                        if (record.value() instanceof TaxiTrip tt) {
                                            return tt.dropTimestamp();
                                        } else {
                                            logger.warn("Wrong input value! record={}", record);
                                            return partitionTime;
                                        }
                                    }));
        var formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
        var zone = ZoneId.of("America/New_York");
        taxiDataStream.selectKey((key, value) -> value.puLocationID())
                      .repartition(Repartitioned.with(Serdes.Integer(), JsonSerde.of(TaxiTrip.class)).withName(Topics.NYC_TAXI_TRIPS_BY_PU_LOCATION_ID.topicName()))
                      .process(TripStatsAggregator::new, Topics.NYC_TAXI_STATS_STORE.topicName())
                      .selectKey((key, value) -> String.format("pu-%d-%s", key, formatter.format(Instant.ofEpochMilli(value.windowStart()).atZone(zone))))
                      .flatMapValues(stats -> List.of(stats.toFare(), stats.toTip(), stats.toPassangers()))
                      .split()
                      .branch((key, stats) -> stats instanceof FareStats, Branched.withConsumer(fareStatsStream -> fareStatsStream.mapValues(stats -> (FareStats) stats)
                                                                                                                                  .to(Topics.NYC_TAXI_DASHBOARD_FARE.topicName(), Produced.with(Serdes.String(), JsonSerde.of(FareStats.class)))))
                      .branch((key, stats) -> stats instanceof TipStats, Branched.withConsumer(tipStatsStreams -> tipStatsStreams.mapValues(stats -> (TipStats) stats)
                                                                                                                                 .to(Topics.NYC_TAXI_DASHBOARD_TIPS.topicName(), Produced.with(Serdes.String(), JsonSerde.of(TipStats.class)))))
                      .branch((key, stats) -> stats instanceof PassengerStats, Branched.withConsumer(tipStatsStreams -> tipStatsStreams.mapValues(stats -> (PassengerStats) stats)
                                                                                                                                       .to(Topics.NYC_TAXI_DASHBOARD_PASSENGERS.topicName(), Produced.with(Serdes.String(), JsonSerde.of(PassengerStats.class)))))
                      .noDefaultBranch();
        taxiDataStream.selectKey((key, value) -> value.doLocationID())
                      .repartition(Repartitioned.with(Serdes.Integer(), JsonSerde.of(TaxiTrip.class)).withName(Topics.NYC_TAXI_TRIPS_BY_DO_LOCATION_ID.topicName()))
                      .process(TripStatsAggregator::new, Topics.NYC_TAXI_STATS_STORE.topicName())
                      .selectKey((key, value) -> String.format("do-%d-%s", key, formatter.format(Instant.ofEpochMilli(value.windowStart()).atZone(zone))))
                      .flatMapValues(stats -> List.of(stats.toFare(), stats.toTip(), stats.toPassangers()))
                      .split()
                      .branch((key, stats) -> stats instanceof FareStats, Branched.withConsumer(fareStatsStream -> fareStatsStream.mapValues(stats -> (FareStats) stats)
                                                                                                                                  .to(Topics.NYC_TAXI_DASHBOARD_FARE.topicName(), Produced.with(Serdes.String(), JsonSerde.of(FareStats.class)))))
                      .branch((key, stats) -> stats instanceof TipStats, Branched.withConsumer(tipStatsStreams -> tipStatsStreams.mapValues(stats -> (TipStats) stats)
                                                                                                                                 .to(Topics.NYC_TAXI_DASHBOARD_TIPS.topicName(), Produced.with(Serdes.String(), JsonSerde.of(TipStats.class)))))
                      .branch((key, stats) -> stats instanceof PassengerStats, Branched.withConsumer(tipStatsStreams -> tipStatsStreams.mapValues(stats -> (PassengerStats) stats)
                                                                                                                                       .to(Topics.NYC_TAXI_DASHBOARD_PASSENGERS.topicName(), Produced.with(Serdes.String(), JsonSerde.of(PassengerStats.class)))))
                      .noDefaultBranch();
        return builder.build();
    }
}
