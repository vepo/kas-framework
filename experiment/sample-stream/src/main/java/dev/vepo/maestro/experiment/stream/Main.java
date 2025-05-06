package dev.vepo.maestro.experiment.stream;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.PunctuationType;
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
import dev.vepo.maestro.experiment.stream.model.VehicleInfo;
import dev.vepo.maestro.experiment.stream.model.VehicleSpeed;
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

        logger.info("Waiting lag grow.... 10 minutes");
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
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, PerformanceMetricsCollector.class.getName());
        try (var maestroStream = create(buildTopology(), props);
                var taskExecutor = Executors.newSingleThreadScheduledExecutor()) {
            var countDown = new CountDownLatch(1);
            maestroStream.cleanUp();

            // taskExecutor.schedule(() -> {
            // }, 5, TimeUnit.MINUTES);
            taskExecutor.schedule(() -> {
                maestroStream.close();
                countDown.countDown();
            }, 15, TimeUnit.MINUTES);
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

    private Streams create(Topology topology, Properties props) {
        return switch (this.type) {
            case VANILLA -> new VanillaStreams(topology, props);
            case MAESTRO -> new MaestroStreams(topology, props);
            default -> throw new IllegalStateException("Invalid type: " + this.type);
        };
    }

    private Topology buildTopology() {
        var builder = new StreamsBuilder();
        // Create state stores
        var positionStoreBuilder = Stores.windowStoreBuilder(Stores.persistentWindowStore(Topics.VEHICLE_INFO_STORE.topicName(),
                                                                                          Duration.ofDays(1),
                                                                                          Duration.ofMinutes(5),
                                                                                          false),
                                                             Serdes.String(),
                                                             JsonSerde.of(VehicleSpeed.class));

        // Register the stores
        builder.addStateStore(positionStoreBuilder);

        // Input stream from vehicles topic
        var vehicleStream = builder.stream(Topics.VEHICLE_MOVIMENT.topicName(),
                                           Consumed.with(Serdes.String(), JsonSerde.of(VehicleInfo.class)));

        vehicleStream.process(() -> new VehicleInfoProcessor(), Topics.VEHICLE_INFO_STORE.topicName())
                     .to(output(), Produced.with(Serdes.String(), JsonSerde.of(VehicleSpeed.class)));
        return builder.build();

    }

    private String output() {
        return Topics.VEHICLE_STATS.topicName() + switch (this.type) {
            case VANILLA -> "-vanilla";
            case MAESTRO -> "-maestro";
        };
    }

    private class VehicleInfoProcessor implements Processor<String, VehicleInfo, String, VehicleSpeed> {

        private WindowStore<String, VehicleSpeed> store;
        private final Duration duration;
        private ProcessorContext<String, VehicleSpeed> context;

        private VehicleInfoProcessor() {
            this.duration = Duration.ofMinutes(5);
        }

        @Override
        public void init(ProcessorContext<String, VehicleSpeed> context) {
            this.context = context;
            store = context.getStateStore(Topics.VEHICLE_INFO_STORE.topicName());
            context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, this::flush);
        }

        private void flush(long timestamp) {
            try (var iterator = store.backwardFetchAll(Instant.ofEpochMilli(0), Instant.ofEpochMilli(timestamp))) {
                while (iterator.hasNext()) {
                    var value = iterator.next();
                    if (Objects.nonNull(value.value)) {
                        context.forward(new Record<>(value.value.id(), value.value, value.key.window().end()));
                        store.put(value.value.id(), null, value.key.window().start());
                    } else {
                        break;
                    }
                }
            }
        }

        @Override
        public void process(Record<String, VehicleInfo> value) {
            var startTime = Instant.ofEpochMilli(value.timestamp()).minus(duration);
            var endTime = Instant.ofEpochMilli(value.timestamp());
            try (var iterator = store.fetch(value.key(), startTime, endTime)) {
                if (iterator.hasNext()) {
                    var storedValue = iterator.next();
                    if (Objects.nonNull(storedValue.value)) {
                        store.put(value.value().id(),
                                  new VehicleSpeed(value.value().id(),
                                                   Math.max(value.value().speed(), storedValue.value.maxSpeed()),
                                                   Math.min(value.value().speed(), storedValue.value.minSpeed()),
                                                   ((storedValue.value.avgSpeed() * storedValue.value.counter()) + value.value().speed())
                                                           / (storedValue.value.counter() + 1),
                                                   storedValue.value.counter() + 1),
                                  storedValue.key);
                        return;
                    }
                }
                store.put(value.value().id(),
                          new VehicleSpeed(value.value().id(),
                                           value.value().speed(),
                                           value.value().speed(),
                                           value.value().speed(),
                                           1),
                          value.value().timestamp());

            }
        }
    }
}
