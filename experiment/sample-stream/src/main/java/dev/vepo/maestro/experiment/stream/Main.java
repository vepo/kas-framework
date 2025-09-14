package dev.vepo.maestro.experiment.stream;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
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

import dev.vepo.kafka.maestro.MaestroStreams;
import dev.vepo.kafka.maestro.Streams;
import dev.vepo.kafka.maestro.VanillaStreams;
import dev.vepo.kafka.maestro.metrics.PerformanceMetricsCollector;
import dev.vepo.maestro.experiment.stream.model.FareStats;
import dev.vepo.maestro.experiment.stream.model.TaxiTrip;
import dev.vepo.maestro.experiment.stream.model.TipStatus;
import dev.vepo.maestro.experiment.stream.serdes.JsonSerde;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "sample-stream", mixinStandardHelpOptions = true, version = "sample-stream 0.1", helpCommand = true)
public class Main implements Runnable {
    public enum Type {
        VANILLA, MAESTRO
    };

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        new CommandLine(new Main()).execute(args);
    }

    @Option(names = {
            "-t",
            "--type" }, required = true)
    Type type;

    @Option(names = "--app-id", required = true)
    String appId;

    @Option(names = "--test-id", required = true)
    String testId;

    @Override
    public void run() {

        logger.info("Waiting lag grow.... 1 minutes");
        try {
            Thread.sleep(Duration.ofMinutes(1).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        logger.info("Starting Streamer");

        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0:9092,kafka-1:9094,kafka-2:9096");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/opt/" + testId + "/state");
        props.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, 300000); // 5 minutes additional retention
        // props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        // StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, PerformanceMetricsCollector.class.getName());
        try (var maestroStream = create(this::buildTopology, props);
                var taskExecutor = Executors.newSingleThreadScheduledExecutor()) {
            var countDown = new CountDownLatch(1);
            // maestroStream.cleanUp();

            // taskExecutor.schedule(() -> {
            // }, 5, TimeUnit.MINUTES);
            taskExecutor.schedule(() -> {
                maestroStream.close();
                countDown.countDown();
            }, 60, TimeUnit.MINUTES);
            maestroStream.start();
            try {
                countDown.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            logger.info("Streamer stopped");
            taskExecutor.shutdown();
        }
    }

    private Streams create(Supplier<Topology> topologyProvider, Properties props) {
        return switch (this.type) {
            case VANILLA -> new VanillaStreams(topologyProvider, props);
            case MAESTRO -> new MaestroStreams(topologyProvider, props);
            default -> throw new IllegalStateException("Invalid type: " + this.type);
        };
    }

    public class FareStatsAggregator implements Processor<Integer, TaxiTrip, String, FareStats> {
        private WindowStore<String, FareStats> store;
        private final Duration duration;
        private ProcessorContext<String, FareStats> context;

        public FareStatsAggregator() {
            this.duration = Duration.ofMinutes(5);
        }

        @Override
        public void init(ProcessorContext<String, FareStats> context) {
            this.context = context;
            store = context.getStateStore(Topics.NYC_TAXI_FARES_STORE.topicName());
        }

        @Override
        public void process(Record<Integer, TaxiTrip> value) {
            var startTime = Instant.ofEpochMilli(value.timestamp()).minus(duration);
            var endTime = Instant.ofEpochMilli(value.timestamp());
            try (var iterator = store.fetch(Integer.toString(value.key()), startTime, endTime)) {
                if (iterator.hasNext()) {
                    var storedValue = iterator.next();
                    var stats = Optional.ofNullable(storedValue.value).orElseGet(FareStats::initializer);
                    store.put(Integer.toString(value.key()), stats.add(value.value()), value.timestamp());
                    context.forward(new Record<>(Integer.toString(value.key()), stats, value.timestamp()));
                } else {
                    var stats = FareStats.initializer().add(value.value());
                    store.put(Integer.toString(value.key()), stats, value.timestamp());
                    context.forward(new Record<>(Integer.toString(value.key()), stats, value.timestamp()));
                }
            }
        }
    }

    public class TipStatsAggregator implements Processor<Integer, TaxiTrip, String, TipStatus> {
        private ProcessorContext<String, TipStatus> context;
        private WindowStore<String, TipStatus> store;
        private final Duration duration;

        public TipStatsAggregator() {
            this.duration = Duration.ofMinutes(5);
        }

        @Override
        public void init(ProcessorContext<String, TipStatus> context) {
            this.context = context;
            store = context.getStateStore(Topics.NYC_TAXI_TIPS_STORE.topicName());
        }

        @Override
        public void process(Record<Integer, TaxiTrip> value) {
            var startTime = Instant.ofEpochMilli(value.timestamp()).minus(duration);
            var endTime = Instant.ofEpochMilli(value.timestamp());
            try (var iterator = store.fetch(Integer.toString(value.key()), startTime, endTime)) {
                if (iterator.hasNext()) {
                    var storedValue = iterator.next();
                    var stats = Optional.ofNullable(storedValue.value).orElseGet(TipStatus::initializer);
                    store.put(Integer.toString(value.key()), stats.add(value.value()), value.timestamp());
                    context.forward(new Record<>(Integer.toString(value.key()), stats, value.timestamp()));
                } else {
                    var stats = TipStatus.initializer().add(value.value());
                    store.put(Integer.toString(value.key()), stats, value.timestamp());
                    context.forward(new Record<>(Integer.toString(value.key()), stats, value.timestamp()));
                }
            }
        }
    }

    private Topology buildTopology() {
        // Use TumblingWindows with longer grace period for counting
        var windowSize = Duration.ofMinutes(1);
        var gracePeriod = Duration.ofMinutes(5); // Increased grace period
        var builder = new StreamsBuilder();
        var fareStoreBuilder = Stores.windowStoreBuilder(Stores.persistentWindowStore(Topics.NYC_TAXI_FARES_STORE.topicName(),
                                                                                      Duration.ofMinutes(15),
                                                                                      Duration.ofMinutes(5),
                                                                     false),
                                                         Serdes.String(),
                                                         JsonSerde.of(FareStats.class));
        var tipsStoreBuilder = Stores.windowStoreBuilder(Stores.persistentWindowStore(Topics.NYC_TAXI_TIPS_STORE.topicName(),
                                                                                      Duration.ofMinutes(15),
                                                                                      Duration.ofMinutes(5),
                                                                     false),
                                                         Serdes.String(),
                                                         JsonSerde.of(TipStatus.class));
        builder.addStateStore(fareStoreBuilder);
        builder.addStateStore(tipsStoreBuilder);
        var taxiDataStream = builder.stream(Topics.NYC_TAXI_TRIPS.topicName(), Consumed.with(Serdes.String(), JsonSerde.of(TaxiTrip.class))
                                    .withTimestampExtractor((record, partitionTime) -> {
                                        if (record.value() instanceof TaxiTrip tt) {
                                            return tt.dropTimestamp();
                                        } else {
                                            logger.warn("Wrong input value! record={}", record);
                                            return partitionTime;
                                        }
                                    }));
        var trupsByPuLocationStream = taxiDataStream.selectKey((key, value) -> value.puLocationID())
                                                    .repartition(Repartitioned.with(Serdes.Integer(), JsonSerde.of(TaxiTrip.class)).withName(Topics.NYC_TAXI_TRIPS_BY_PU_LOCATION_ID.topicName()));
        trupsByPuLocationStream.process(FareStatsAggregator::new, Topics.NYC_TAXI_FARES_STORE.topicName())
                               .to(Topics.NYC_TAXI_DASHBOARD_FARE.topicName(), Produced.with(Serdes.String(), JsonSerde.of(FareStats.class)));
        trupsByPuLocationStream.process(TipStatsAggregator::new, Topics.NYC_TAXI_TIPS_STORE.topicName())
                               .to(Topics.NYC_TAXI_DASHBOARD_TIPS.topicName(), Produced.with(Serdes.String(), JsonSerde.of(TipStatus.class)));
        return builder.build();
    }
}
