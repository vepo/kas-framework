package io.vepo.kafka.maestro;

import static io.vepo.kafka.maestro.MaestroConfigs.DEFAULT_MAESTRO_TUNING_FREQUENCY_MS;
import static io.vepo.kafka.maestro.MaestroConfigs.DEFAULT_NUM_STREAM_THREADS;
import static io.vepo.kafka.maestro.MaestroConfigs.MAESTRO_TUNING_FREQUENCY_MS_CONFIG;
import static io.vepo.kafka.maestro.MaestroConfigs.NUM_STREAM_THREADS_CONFIG;

import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;

import io.vepo.kafka.maestro.metrics.MaestroMetricsCollector;
import io.vepo.kafka.maestro.metrics.PerformanceOptimizer;

public class MaestroStream extends KafkaStreams {

    public static MaestroStream create(Topology topology, Properties props) {
        try {
            if (props.containsKey(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG)) {
                props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, props.get(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG) + "," + MaestroMetricsCollector.class.getName());
            } else {
                props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MaestroMetricsCollector.class.getName());
            }
            var optimizer = new PerformanceOptimizer();
            props.put(MaestroConfigs.MAESTRO_PERFORMANCE_OPTIMIZER_CONFIG, optimizer);
            return new MaestroStream(topology, props, new MaestroClientSupplier(optimizer));
        } catch (StreamsException se) {
            throw new RuntimeException("Error creating KafkaStreams", se);
        }
    }

    private final ScheduledExecutorService executors;
    private final int tuningFrequency;
    private ScheduledFuture<?> scheduledTune;
    private final int threadCount;

    private MaestroStream(final Topology topology, final Properties props) {
        super(topology, props);
        throw new UnsupportedOperationException("Use factory method!");
    }

    private MaestroStream(final Topology topology,
                          final Properties props,
                          final KafkaClientSupplier clientSupplier) {
        super(topology, props, clientSupplier);
        tuningFrequency = Integer.parseInt(props.getProperty(MAESTRO_TUNING_FREQUENCY_MS_CONFIG, Integer.toString(DEFAULT_MAESTRO_TUNING_FREQUENCY_MS)));
        threadCount = Integer.parseInt(props.getProperty(NUM_STREAM_THREADS_CONFIG, Integer.toString(DEFAULT_NUM_STREAM_THREADS)));
        executors = Executors.newSingleThreadScheduledExecutor();
        if (threadCount == 1) {
            throw new InvalidParameterException(String.format("Invalid parameter \"%s\" value! It should be greater than 1", NUM_STREAM_THREADS_CONFIG));
        }
        scheduledTune = null;
    }

    private MaestroStream(final Topology topology,
                          final Properties props,
                          final Time time) {
        super(topology, props, time);
        throw new UnsupportedOperationException("Use factory method!");
    }

    private MaestroStream(final Topology topology,
                          final Properties props,
                          final KafkaClientSupplier clientSupplier,
                          final Time time) {
        super(topology, props, clientSupplier, time);
        throw new UnsupportedOperationException("Use factory method!");
    }

    private MaestroStream(final Topology topology,
                          final StreamsConfig applicationConfigs) {
        super(topology, applicationConfigs);
        throw new UnsupportedOperationException("Use factory method!");
    }

    private MaestroStream(final Topology topology,
                          final StreamsConfig applicationConfigs,
                          final KafkaClientSupplier clientSupplier) {
        super(topology, applicationConfigs, clientSupplier);
        throw new UnsupportedOperationException("Use factory method!");
    }

    @Override
    public synchronized void start() throws IllegalStateException, StreamsException {
        scheduledTune = executors.scheduleAtFixedRate(this::tune, tuningFrequency, tuningFrequency, TimeUnit.MILLISECONDS);
        super.start();
    }

    @Override
    public void close() {
        if (Objects.nonNull(scheduledTune)) {
            scheduledTune.cancel(false);
        }
        executors.shutdown();
        try {
            executors.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executors.shutdownNow();
        super.close();
    }

    @Override
    public synchronized boolean close(CloseOptions options) throws IllegalArgumentException {
        executors.shutdown();
        return super.close(options);
    }

    @Override
    public synchronized boolean close(Duration timeout) throws IllegalArgumentException {
        executors.shutdown();
        return super.close(timeout);
    }

    private void tune() {
    }

}
