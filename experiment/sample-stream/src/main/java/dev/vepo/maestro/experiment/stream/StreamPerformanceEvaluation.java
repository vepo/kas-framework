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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.producer.ProducerConfig;
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
    private static final int TEST_DURATION_IN_MINUTES = 30;
    private static final CommandClient COMMAND = new CommandClient();
    private static final String[] TOPICS = new String[] { "raw-output",
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
                                                          "%app-id%-nyc-taxi-pu-stats-store-changelog",
                                                          "%app-id%-nyc-taxi-do-stats-store-changelog",
                                                          "%app-id%-nyc-taxi-trips-by-pu-location-id-repartition",
                                                          "%app-id%-nyc-taxi-trips-by-do-location-id-repartition",
                                                          "%app-id%-trips-per-hour-repartition" };
    private record Execution(String appId,
                             String useCase,
                             String useCaseArgument,
                             Type type,
                             TopologyDefinition topology,
                             List<Class<? extends AdapterRule>> rules) {
        private Execution(String appId,
                             String useCase,
                             String useCaseArgument,
                             Type type,
                             TopologyDefinition topology) {
            this(appId, useCase, useCaseArgument, type,topology, null);
        }

        public void exec() {
            try {
                createTopics();
                COMMAND.sendCommand(Command.START, useCase, useCaseArgument);
                var evaluation = new StreamPerformanceEvaluation(type, appId, topology, rules);
                evaluation.run();
                COMMAND.sendCommand(Command.STOP);
                sleep(Duration.ofMinutes(1));
            } catch (Exception ex) {
                logger.error("Error executing test case: {}", this, ex);
                try {
                    COMMAND.sendCommand(Command.STOP);
                    COMMAND.sendCommand(Command.DONE);
                } catch (Exception ex2) {
                    logger.error("Error stopping producer!", ex2);
                }
                System.exit(-1);
            }
        }

        private void createTopics() throws InterruptedException, ExecutionException {
            var props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0:9092,kafka-1:9094,kafka-2:9096");

            try (var adminClient = AdminClient.create(props)) {
                var existingConsumerGroups = adminClient.listConsumerGroups()
                                                        .all()
                                                        .get()
                                                        .stream()
                                                        .map(ConsumerGroupListing::groupId)
                                                        .toList();

                if (!existingConsumerGroups.isEmpty()) {
                    // Try to remove members from groups first
                    existingConsumerGroups.forEach(groupId -> {
                        try {
                            logger.info("Removing members from consumer group: {}", groupId);
                            var removeResult = adminClient.removeMembersFromConsumerGroup(groupId, new RemoveMembersFromConsumerGroupOptions()).all().get();
                            // This is a simplified approach - you might need to handle each group individually
                            logger.info("Members removed! {}", removeResult);
                        } catch (Exception e) {
                            logger.warn("Could not remove members from consumer groups: {}", e.getMessage());
                        }
                    });
                    var deletedGroups = adminClient.deleteConsumerGroups(existingConsumerGroups);
                    deletedGroups.all().get();
                    logger.info("Consumer groups deleted! groups={}", existingConsumerGroups);
                }
                var existingTopicsNames = adminClient.listTopics()
                                                     .listings().get().stream()
                                                     .map(TopicListing::name)
                                                     .collect(Collectors.toList());
                if (!existingTopicsNames.isEmpty()) {
                    logger.info("Deleting topics: {}", existingTopicsNames);

                    // Delete all topics
                    DeleteTopicsResult deleteResult = adminClient.deleteTopics(existingTopicsNames);
                    deleteResult.all().get(); // Wait for deletion to complete

                    logger.info("Successfully deleted topics. {}", existingTopicsNames);
                }
                // Create topics
                var topicNames = Stream.of(TOPICS)
                                       .map(t -> t.replace("%app-id%", appId))
                                       .map(topic -> new NewTopic(topic, 12, (short) 1))
                                       .toList();
                var result = adminClient.createTopics(topicNames);

                // Wait for completion and get results
                result.all().get();
                logger.info("All topics created successfully");
            }
        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        sleep(Duration.ofSeconds(5));
        COMMAND.startConnection("producer", 7777);
        // base stats
        Stream.of(new Execution("stats-simple-warmup", "STATS", "", Type.VANILLA, TopologyDefinition.STATS),
                  new Execution("stats-simple-baseline", "STATS", "", Type.VANILLA, TopologyDefinition.STATS),
                  new Execution("stats-simple-all", "STATS", "", Type.MAESTRO, TopologyDefinition.STATS, List.of(ThreadAllocationRule.class,
                                                                                                                 AdjustConsumerFetchSizeRule.class,
                                                                                                                 UseCompressionOnProducerRule.class,
                                                                                                                 BatchProducerRule.class)),
                  new Execution("stats-simple-thread-allocation", "STATS", "", Type.MAESTRO, TopologyDefinition.STATS, List.of(ThreadAllocationRule.class)),
                  new Execution("stats-simple-fetch-size", "STATS", "", Type.MAESTRO, TopologyDefinition.STATS, List.of(AdjustConsumerFetchSizeRule.class)),
                  new Execution("stats-simple-compression", "STATS", "", Type.MAESTRO, TopologyDefinition.STATS, List.of(UseCompressionOnProducerRule.class)),
                  new Execution("stats-simple-batch", "STATS", "", Type.MAESTRO, TopologyDefinition.STATS, List.of(BatchProducerRule.class)),
                  // Stats with more topics
                  new Execution("stats-plus-warmup", "STATS", "", Type.VANILLA, TopologyDefinition.STATS_MORE_OUTPUT),
                  new Execution("stats-plus-baseline", "STATS", "", Type.VANILLA, TopologyDefinition.STATS_MORE_OUTPUT),
                  new Execution("stats-plus-all", "STATS", "", Type.MAESTRO, TopologyDefinition.STATS_MORE_OUTPUT, List.of(ThreadAllocationRule.class,
                                                                                                                           AdjustConsumerFetchSizeRule.class,
                                                                                                                           UseCompressionOnProducerRule.class,
                                                                                                                           BatchProducerRule.class)),
                  new Execution("stats-plus-thread-allocation", "STATS", "", Type.MAESTRO, TopologyDefinition.STATS_MORE_OUTPUT, List.of(ThreadAllocationRule.class)),
                  new Execution("stats-plus-fetch-size", "STATS", "", Type.MAESTRO, TopologyDefinition.STATS_MORE_OUTPUT, List.of(AdjustConsumerFetchSizeRule.class)),
                  new Execution("stats-plus-compression", "STATS", "", Type.MAESTRO, TopologyDefinition.STATS_MORE_OUTPUT, List.of(UseCompressionOnProducerRule.class)),
                  new Execution("stats-plus-batch", "STATS", "", Type.MAESTRO, TopologyDefinition.STATS_MORE_OUTPUT, List.of(BatchProducerRule.class)),
                  // RAW - 100
                  new Execution("raw-100-warmup", "RAW", "100", Type.VANILLA, TopologyDefinition.PASSTHROUGH),
                  new Execution("raw-100-baseline", "RAW", "100", Type.VANILLA, TopologyDefinition.PASSTHROUGH),
                  new Execution("raw-100-all", "RAW", "100", Type.MAESTRO, TopologyDefinition.PASSTHROUGH, List.of(ThreadAllocationRule.class,
                                                                                                                   AdjustConsumerFetchSizeRule.class,
                                                                                                                   UseCompressionOnProducerRule.class,
                                                                                                                   BatchProducerRule.class)),
                  new Execution("raw-100-thread-allocation", "RAW", "100", Type.MAESTRO, TopologyDefinition.PASSTHROUGH, List.of(ThreadAllocationRule.class)),
                  new Execution("raw-100-fetch-size", "RAW", "100", Type.MAESTRO, TopologyDefinition.PASSTHROUGH, List.of(AdjustConsumerFetchSizeRule.class)),
                  new Execution("raw-100-compression", "RAW", "100", Type.MAESTRO, TopologyDefinition.PASSTHROUGH, List.of(UseCompressionOnProducerRule.class)),
                  new Execution("raw-100-batch", "RAW", "100", Type.MAESTRO, TopologyDefinition.PASSTHROUGH, List.of(BatchProducerRule.class)),
                  // RAW - 250
                  new Execution("raw-250-warmup", "RAW", "250", Type.VANILLA, TopologyDefinition.PASSTHROUGH),
                  new Execution("raw-250-baseline", "RAW", "250", Type.VANILLA, TopologyDefinition.PASSTHROUGH),
                  new Execution("raw-250-all", "RAW", "250", Type.MAESTRO, TopologyDefinition.PASSTHROUGH, List.of(ThreadAllocationRule.class,
                                                                                                                   AdjustConsumerFetchSizeRule.class,
                                                                                                                   UseCompressionOnProducerRule.class,
                                                                                                                   BatchProducerRule.class)),
                  new Execution("raw-250-thread-allocation", "RAW", "250", Type.MAESTRO, TopologyDefinition.PASSTHROUGH, List.of(ThreadAllocationRule.class)),
                  new Execution("raw-250-fetch-size", "RAW", "250", Type.MAESTRO, TopologyDefinition.PASSTHROUGH, List.of(AdjustConsumerFetchSizeRule.class)),
                  new Execution("raw-250-compression", "RAW", "250", Type.MAESTRO, TopologyDefinition.PASSTHROUGH, List.of(UseCompressionOnProducerRule.class)),
                  new Execution("raw-250-batch", "RAW", "250", Type.MAESTRO, TopologyDefinition.PASSTHROUGH, List.of(BatchProducerRule.class)),
                  // RAW - 500
                  new Execution("raw-500-warmup", "RAW", "500", Type.VANILLA, TopologyDefinition.PASSTHROUGH),
                  new Execution("raw-500-baseline", "RAW", "500", Type.VANILLA, TopologyDefinition.PASSTHROUGH),
                  new Execution("raw-500-all", "RAW", "500", Type.MAESTRO, TopologyDefinition.PASSTHROUGH, List.of(ThreadAllocationRule.class,
                                                                                                                   AdjustConsumerFetchSizeRule.class,
                                                                                                                   UseCompressionOnProducerRule.class,
                                                                                                                   BatchProducerRule.class)),
                  new Execution("raw-500-thread-allocation", "RAW", "500", Type.MAESTRO, TopologyDefinition.PASSTHROUGH, List.of(ThreadAllocationRule.class)),
                  new Execution("raw-500-fetch-size", "RAW", "500", Type.MAESTRO, TopologyDefinition.PASSTHROUGH, List.of(AdjustConsumerFetchSizeRule.class)),
                  new Execution("raw-500-compression", "RAW", "500", Type.MAESTRO, TopologyDefinition.PASSTHROUGH, List.of(UseCompressionOnProducerRule.class)),
                  new Execution("raw-500-batch", "RAW", "500", Type.MAESTRO, TopologyDefinition.PASSTHROUGH, List.of(BatchProducerRule.class)))
              .forEachOrdered(Execution::exec);
        COMMAND.sendCommand(Command.DONE);
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
    }

    @Override
    public void run() {
        logger.info("Waiting lag grow.... 1 minutes");
        sleep(Duration.ofMinutes(1));
        logger.info("Starting Streamer");

        var stateDir = Paths.get("/opt", testId, "state");
        cleanUpFolder(stateDir.toFile());
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0:9092,kafka-1:9094,kafka-2:9096");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, testId);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        props.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, Long.MAX_VALUE);
        props.put(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG, 40000 * 3); // 3 times
        props.put(StreamsConfig.producerPrefix(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG), (40000 * 3) + 100); // 3 times
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        props.put("metrics.stats.folder", Paths.get(System.getenv("STATS_FOLDER"), testId).toAbsolutePath().toString());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/opt/" + testId + "/state");
        if (Objects.nonNull(adapterRules)) {
            props.put(MaestroConfigs.ADAPTER_RULE_CLASSES_CONFIG, adapterRules);
        }
        props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, PerformanceMetricsCollector.class.getName());
        try (var maestroStream = create(this::buildTopology, props);
                var taskExecutor = Executors.newSingleThreadScheduledExecutor()) {
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

    private static void sleep(Duration timeout) {
        try {
            Thread.sleep(timeout.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
        private final String topicName;
        private final Duration duration;
        private final String topicName;
        private ProcessorContext<Integer, TripStats> context;

        public TripStatsAggregator(String topicName) {
            this.topicName = topicName;
            this.duration = Duration.ofMinutes(1);
        }

        @Override
        public void init(ProcessorContext<Integer, TripStats> context) {
            this.context = context;
            store = context.getStateStore(topicName);
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

    private Topology buildPassthroughTopology() {
        var builder = new StreamsBuilder();
        builder.stream(Topics.RAW_DATA_INPUT.topicName(), Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
               .mapValues(input -> {
                     if (input == null) {
                        return createEmpty64x64Image();
                    }

                    try {
                        return resizeTo64x64(input);
                    } catch (Exception e) {
                        // Log error and return empty image
                        logger.error("Error resizing image!", e);
                        return createEmpty64x64Image();
                    }
                })
               .selectKey((key, value) -> md5Hash(value))
               .to(Topics.RAW_DATA_OUTPUT.topicName(), Produced.with(Serdes.String(), Serdes.ByteArray()));
        return builder.build();
    }

    private Topology buildStatsTopology() {
        // Use TumblingWindows with longer grace period for counting
        var windowSize = Duration.ofMinutes(1);
        var gracePeriod = Duration.ofMinutes(5); // Increased grace period
        var retentionPeriod = windowSize.plus(gracePeriod).plus(Duration.ofMinutes(15)); // Clean up old data
        var builder = new StreamsBuilder();
        var statsStoreBuilder = Stores.windowStoreBuilder(Stores.persistentWindowStore(Topics.NYC_TAXI_PU_STATS_STORE.topicName(),
                                                                                       retentionPeriod,
                                                                                       windowSize,
                                                                      false),
                                                          Serdes.Integer(),
                                                          JsonSerde.of(TripStats.class));
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
                      .process(() -> new TripStatsAggregator(Topics.NYC_TAXI_PU_STATS_STORE.topicName()), Topics.NYC_TAXI_PU_STATS_STORE.topicName())
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
        builder.addStateStore(Stores.windowStoreBuilder(Stores.persistentWindowStore(Topics.NYC_TAXI_PU_STATS_STORE.topicName(),
                                                                                       retentionPeriod,
                                                                                       windowSize,
                                                                      false),
                                                          Serdes.Integer(),
                                                          JsonSerde.of(TripStats.class)))
               .addStateStore(Stores.windowStoreBuilder(Stores.persistentWindowStore(Topics.NYC_TAXI_DO_STATS_STORE.topicName(),
                                                                                       retentionPeriod,
                                                                                       windowSize,
                                                                      false),
                                                          Serdes.Integer(),
                                                          JsonSerde.of(TripStats.class)));
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
                      .process(() -> new TripStatsAggregator(Topics.NYC_TAXI_PU_STATS_STORE.topicName()), Topics.NYC_TAXI_PU_STATS_STORE.topicName())
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
                      .process(() -> new TripStatsAggregator(Topics.NYC_TAXI_DO_STATS_STORE.topicName()), Topics.NYC_TAXI_DO_STATS_STORE.topicName())
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
