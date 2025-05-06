package dev.vepo.kafka.maestro.adapter;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;

import dev.vepo.kafka.maestro.MaestroConfigs;
import dev.vepo.kafka.maestro.metrics.MetricListener;
import dev.vepo.kafka.maestro.metrics.PerformanceMetric;

public class Adapter implements MetricListener, Configurable {
    private final Map<MetricKey, MetricValues> metrics;

    private final ScheduledExecutorService taskExecutor;
    private KafkaStreams streams;
    private StreamsContext context;
    private final List<AdapterRule> rules;
    private int maxHistorySize;
    private int minHistorySize;

    public Adapter() {
        this.maxHistorySize = MaestroConfigs.DEFAULT_MAESTRO_ADAPTER_HISTORY_SIZE_MAX;
        this.minHistorySize = MaestroConfigs.DEFAULT_MAESTRO_ADAPTER_HISTORY_SIZE_MIN;
        this.taskExecutor = Executors.newSingleThreadScheduledExecutor();
        this.metrics = Collections.synchronizedMap(new HashMap<>());
        this.context = new StreamsContext(State.CREATED, ThroughputState.INITIALIZING, ResourcesState.AVAILABLE, metrics);
        this.rules = List.of(new ThroughputAnalyzerRule(), new ThreadAllocationRule());
    }

    @Override
    public void close() throws Exception {
        taskExecutor.shutdown();
        if (!taskExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
            taskExecutor.shutdownNow();
        }
        taskExecutor.close();
    }

    @Override
    public void configure(Map<String, ?> props) {
        var configs = new MaestroConfigs(props);
        this.maxHistorySize = configs.getInt(MaestroConfigs.MAESTRO_ADAPTER_HISTORY_SIZE_MAX_CONFIG);
        this.minHistorySize = configs.getInt(MaestroConfigs.MAESTRO_ADAPTER_HISTORY_SIZE_MIN_CONFIG);
        var frequencyMs = configs.getLong(MaestroConfigs.MAESTRO_ADAPTER_FREQUENCY_MS_CONFIG);
        taskExecutor.scheduleAtFixedRate(this::verify, frequencyMs, frequencyMs, TimeUnit.MILLISECONDS);
    }

    public void setup(KafkaStreams streams) {
        this.streams = streams;
        this.streams.setStateListener(this::kafkaStreamsStateChanged);
    }

    private synchronized void kafkaStreamsStateChanged(KafkaStreams.State newState, KafkaStreams.State oldState) {
       this.context = context.withStreams(newState);
    }

    @Override
    public void feed(PerformanceMetric metric) {
        switch (metric.context()) {
            case PARTITION:
                metrics.computeIfAbsent(MetricKey.partition(metric.name(), metric.topic(), metric.partition()),
                                        (_key) -> new MetricValues(this.minHistorySize, this.maxHistorySize))
                       .add(metric.value(), metric.timestamp());
                break;

            default:
                break;
        }
    }

    private synchronized void verify() {
        switch (context.streams()) {
            case CREATED:
            case REBALANCING:
                // nothing
                break;
            case RUNNING:
                this.context = rules.stream()
                                    .sequential()
                                    .reduce(this.context, // Initial value
                                            (ctx, rule) -> rule.evaluate(ctx), // Accumulator
                                            (ctx1, ctx2) -> ctx2 // Combiner (not used in sequential streams)
                                    );
            case PENDING_SHUTDOWN:
            case NOT_RUNNING:
            case PENDING_ERROR:
            case ERROR:
                // nothing
                break;
            default:
                // nothing to do!
        }
    }
}