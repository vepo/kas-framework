package dev.vepo.kafka.maestro.adapter;

import static dev.vepo.kafka.maestro.MaestroConfigs.DEFAULT_MAESTRO_ADAPTER_HISTORY_SIZE_MAX;
import static dev.vepo.kafka.maestro.MaestroConfigs.DEFAULT_MAESTRO_ADAPTER_HISTORY_SIZE_MIN;
import static dev.vepo.kafka.maestro.MaestroConfigs.MAESTRO_ADAPTER_FREQUENCY_MS_CONFIG;
import static dev.vepo.kafka.maestro.MaestroConfigs.MAESTRO_ADAPTER_HISTORY_SIZE_MAX_CONFIG;
import static dev.vepo.kafka.maestro.MaestroConfigs.MAESTRO_ADAPTER_HISTORY_SIZE_MIN_CONFIG;
import static dev.vepo.kafka.maestro.adapter.stats.StatsKey.broker;
import static dev.vepo.kafka.maestro.adapter.stats.StatsKey.client;
import static dev.vepo.kafka.maestro.adapter.stats.StatsKey.jvm;
import static dev.vepo.kafka.maestro.adapter.stats.StatsKey.partition;
import static dev.vepo.kafka.maestro.adapter.stats.StatsKey.topic;
import static dev.vepo.kafka.maestro.metrics.MetricsRecorder.recordEvent;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.vepo.kafka.maestro.MaestroConfigs;
import dev.vepo.kafka.maestro.Streams;
import dev.vepo.kafka.maestro.adapter.context.ResourcesState;
import dev.vepo.kafka.maestro.adapter.context.StreamsContext;
import dev.vepo.kafka.maestro.adapter.context.ThroughputState;
import dev.vepo.kafka.maestro.adapter.rules.AdapterRule;
import dev.vepo.kafka.maestro.adapter.rules.AdjustConsumerFetchSizeRule;
import dev.vepo.kafka.maestro.adapter.rules.BatchProducerRule;
import dev.vepo.kafka.maestro.adapter.rules.AdapterRule.RequiredChanges;
import dev.vepo.kafka.maestro.adapter.rules.ThreadAllocationRule;
import dev.vepo.kafka.maestro.adapter.rules.ThroughputAnalyzerRule;
import dev.vepo.kafka.maestro.adapter.rules.UseCompressionOnProducerRule;
import dev.vepo.kafka.maestro.adapter.stats.StatsKey;
import dev.vepo.kafka.maestro.adapter.stats.StatsValues;
import dev.vepo.kafka.maestro.metrics.BrokerMetricsCollector;
import dev.vepo.kafka.maestro.metrics.MetricListener;
import dev.vepo.kafka.maestro.metrics.PerformanceMetric;
import dev.vepo.kafka.maestro.metrics.MetricsRecorder.Event;

public class Adapter implements MetricListener, Configurable, StateListener {
    private static final Logger logger = LoggerFactory.getLogger(Adapter.class);
    private final Map<StatsKey, StatsValues> metrics;

    private final ScheduledExecutorService taskExecutor;
    private StreamsContext context;
    private final List<AdapterRule> rules;
    private int maxHistorySize;
    private int minHistorySize;
    private BrokerMetricsCollector clusterMetricsCollector;

    public Adapter() {
        this.maxHistorySize = DEFAULT_MAESTRO_ADAPTER_HISTORY_SIZE_MAX;
        this.minHistorySize = DEFAULT_MAESTRO_ADAPTER_HISTORY_SIZE_MIN;
        this.taskExecutor = Executors.newSingleThreadScheduledExecutor(r -> new Thread("Adapter"));
        this.metrics = Collections.synchronizedMap(new HashMap<>());
        this.context = new StreamsContext(State.CREATED, ThroughputState.INITIALIZING, ResourcesState.AVAILABLE, metrics, null);
        this.rules = new ArrayList<>();
        this.rules.add(new ThroughputAnalyzerRule());
        // this.rules = List.of(new ThroughputAnalyzerRule(), 
        //                      new ThreadAllocationRule(),
        //                      new AdjustConsumerFetchSizeRule(),
        //                      new UseCompressionOnProducerRule(),
        //                      new BatchProducerRule());
        // this.rules = List.of(new ThroughputAnalyzerRule(), 
        //                      new ThreadAllocationRule());
        // this.rules = List.of(new ThroughputAnalyzerRule(), 
        //                      new AdjustConsumerFetchSizeRule());
        // this.rules = List.of(new ThroughputAnalyzerRule(),
        //                      new UseCompressionOnProducerRule());
        // this.rules = List.of(new ThroughputAnalyzerRule(),
        //                      new BatchProducerRule());
        this.clusterMetricsCollector = null;
    }

    @Override
    public void close() throws Exception {
        taskExecutor.shutdown();
        if (!taskExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
            taskExecutor.shutdownNow();
        }
        if (Objects.nonNull(clusterMetricsCollector)) {
            clusterMetricsCollector.close();
        }
    }

    @Override
    public void configure(Map<String, ?> props) {
        logger.info("Setting up adapter with {}", props);
        var configs = new MaestroConfigs(props);
        var configuredRules = configs.getConfiguredInstances(MaestroConfigs.ADAPTER_RULE_CLASSES_CONFIG, AdapterRule.class);
        if (Objects.isNull(configuredRules) || configuredRules.isEmpty()) {
            this.rules.add(new ThreadAllocationRule());
            this.rules.add(new AdjustConsumerFetchSizeRule());
            this.rules.add(new UseCompressionOnProducerRule());
            this.rules.add(new BatchProducerRule());
        } else {
            this.rules.addAll(configuredRules);
        }
        logger.info("Rules: {}", rules);
        this.maxHistorySize = configs.getInt(MAESTRO_ADAPTER_HISTORY_SIZE_MAX_CONFIG);
        this.minHistorySize = configs.getInt(MAESTRO_ADAPTER_HISTORY_SIZE_MIN_CONFIG);
        var frequencyMs = configs.getLong(MAESTRO_ADAPTER_FREQUENCY_MS_CONFIG);
        logger.info("Adapter configured! minHistory={} maxHistory={} frequency={}", minHistorySize, maxHistorySize, frequencyMs);
        taskExecutor.scheduleAtFixedRate(this::verify, frequencyMs, frequencyMs, TimeUnit.MILLISECONDS);       
    }

    public void setup(Streams streams) {
        logger.info("Configuring Streams for Adapter...");
        this.context = context.withInstance(streams);
        if (Objects.isNull(this.clusterMetricsCollector)) {
            this.clusterMetricsCollector = new BrokerMetricsCollector(this, streams.inputTopics());
            this.clusterMetricsCollector.configure(streams.originalConfigs());
        }
    }

    @Override
    public synchronized void onChange(State newState, State oldState) {
        logger.info("Stream state changed! newState={} oldState={}", newState, oldState);
        this.context = context.withStreams(newState);
    }

    @Override
    public synchronized void feed(PerformanceMetric metric) {
        logger.trace("New metric receive: {}", metric);
        switch (metric.context()) {
            case PARTITION:
                metrics.computeIfAbsent(partition(metric.name(), metric.topic(), metric.partition()), (_key) -> new StatsValues(this.minHistorySize, this.maxHistorySize))
                       .add(metric.value(), metric.timestamp());
                break;
            case CLIENT:
                metrics.computeIfAbsent(client(metric.name(), metric.clientId()), (_key) -> new StatsValues(this.minHistorySize, this.maxHistorySize))
                       .add(metric.value(), metric.timestamp());
                break;
            case TOPIC:
                metrics.computeIfAbsent(topic(metric.name(), metric.topic(), "broker".equalsIgnoreCase(metric.scope()), metric.clientId()), (_key) -> new StatsValues(this.minHistorySize, this.maxHistorySize))
                       .add(metric.value(), metric.timestamp());
                break;
            case BROKER:
                metrics.computeIfAbsent(broker(metric.name()), (_key) -> new StatsValues(this.minHistorySize, this.maxHistorySize))
                       .add(metric.value(), metric.timestamp());
                break;
            case JVM:
                metrics.computeIfAbsent(jvm(metric.name()), (_key) -> new StatsValues(this.minHistorySize, this.maxHistorySize))
                       .add(metric.value(), metric.timestamp());
                break;
            default:
                break;
        }
    }

    private synchronized void verify() {
        logger.info("Verifying adapter... status={}", context.streams());
        try {
            switch (context.streams()) {
                case CREATED:
                case REBALANCING:
                    // nothing
                    break;
                case RUNNING:
                    recordEvent(Event.CHECK);
                    var changes = new RequiredChanges();
                    logger.info("""
                                Evaluating adapting rules...
                                Current state: {}
                                Accumulated lag: {}
                                Accumulated throughput: {}
                                """, 
                                context.throughput(), 
                                String.format("%.2f", context.accumulatedLag()),
                                String.format("%.2f", context.accumulatedThroughput()));
                    this.context = rules.stream()
                                        .sequential()
                                        .reduce(this.context, // Initial value
                                                (ctx, rule) -> rule.evaluate(ctx, changes), // Accumulator
                                                (ctx1, ctx2) -> ctx2 // Combiner (not used in sequential streams)
                                        );
                    logger.info("Changes required {}!", changes);
                    if (changes.changeRequired()) {
                        // restart need to run async to do not block itself
                        this.context = this.context.reset();
                        if (changes.useNewConfigs()) {
                            var originalConfigs = context.instance().originalConfigs();
                            if (changes.shouldIncreaseThreads()) {
                                originalConfigs.put(NUM_STREAM_THREADS_CONFIG, changes.numThreads());
                            }
                            originalConfigs.putAll(changes.newConfigs());
                            var props = new Properties();
                            props.putAll(originalConfigs);
                            context.instance().restart(props);
                            recordEvent(Event.RESTART, props);
                        } else if (changes.shouldIncreaseThreads()) {
                            context.instance().addNewThreads(changes.numThreads());
                            recordEvent(Event.APPLY, Map.of(NUM_STREAM_THREADS_CONFIG, changes.numThreads()));
                        }
                    }
                    break;
                case PENDING_SHUTDOWN:
                case NOT_RUNNING:
                case PENDING_ERROR:
                case ERROR:
                    // nothing
                    break;
                default:
                    // nothing to do!
            }
        } catch (Exception e) {
            logger.error("Error while evaluating rules!", e);
        }
    }
}