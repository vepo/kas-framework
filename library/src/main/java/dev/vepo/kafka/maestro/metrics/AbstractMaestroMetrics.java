package dev.vepo.kafka.maestro.metrics;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.CLIENT_ID_CONFIG;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.vepo.kafka.maestro.MaestroConfigs;
import dev.vepo.kafka.maestro.metrics.PerformanceMetric.Context;

public abstract class AbstractMaestroMetrics implements MetricsReporter {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMaestroMetrics.class);
    private final Object lock;
    private final Map<MetricKey, KafkaMetric> metrics;
    private final ScheduledExecutorService executor;
    private String prefix;
    private EnvironmentMetrics environmentMetrics;
    private ScheduledFuture<?> mainCollector;
    private ScheduledFuture<?> jvmCollector;

    protected AbstractMaestroMetrics(Object lock) {
        this.lock = lock;
        prefix = "";
        metrics = Collections.synchronizedMap(new HashMap<>());
        executor = Executors.newSingleThreadScheduledExecutor();
        this.environmentMetrics = null;
        this.mainCollector = null;
        this.jvmCollector = null;
    }

    protected abstract void process(PerformanceMetric metric);

    private boolean isMainThread(Map<String, ?> configs) {
        if (configs.containsKey(APPLICATION_ID_CONFIG) && configs.containsKey(CLIENT_ID_CONFIG)) {
            var clientId = (String) configs.get(CLIENT_ID_CONFIG);
            return Objects.nonNull(clientId) && !clientId.endsWith("-admin") && !clientId.endsWith("-producer")
                    && !clientId.endsWith("-consumer");
        }
        return false;
    }

    private void collectJvmMetrics() {
        process(new PerformanceMetric(Context.JVM, "cpu-used", "jvm", environmentMetrics.cpuUsed()));
        process(new PerformanceMetric(Context.JVM, "cpu-total", "jvm", environmentMetrics.cpuTotal()));
        process(new PerformanceMetric(Context.JVM, "memory-total", "jvm", environmentMetrics.memoryTotal()));
        process(new PerformanceMetric(Context.JVM, "memory-used", "jvm", environmentMetrics.memoryUsed()));
    }

    private void collectMainMetrics() {
        metrics.forEach((key, value) -> {
            if (value.metricValue() instanceof Number statValue
                    && (!(statValue instanceof Double) || !Double.isNaN((double) statValue))) {
                if (value.metricName().tags().containsKey("topic")
                        && value.metricName().tags().containsKey("partition")) {
                    process(new PerformanceMetric(key.scope(),
                            key.name(),
                            value.metricName().tags().get("client-id"),
                            statValue,
                            value.metricName().tags().get("topic"),
                            Integer.valueOf(value.metricName().tags().get("partition"))));
                } else if (value.metricName().tags().containsKey("topic")) {
                    process(new PerformanceMetric(key.scope(),
                            key.name(),
                            value.metricName().tags().get("client-id"),
                            statValue,
                            value.metricName().tags().get("topic")));
                } else {
                    process(new PerformanceMetric(key.scope(),
                            key.name(),
                            value.metricName().tags().get("client-id"),
                            statValue));
                }
            }
        });

    }

    @Override
    public void configure(Map<String, ?> configs) {
        var mConfigs = new MaestroConfigs(configs);
        var frequencyInMs = mConfigs.getLong(MaestroConfigs.MAESTRO_METRICS_COLLECTOR_FREQUENCY_MS_CONFIG);
        mainCollector = executor.scheduleAtFixedRate(this::collectMainMetrics, frequencyInMs, frequencyInMs, TimeUnit.MILLISECONDS);
        if (isMainThread(configs)) {
            logger.info("Configuring main thread metrics collector: configs: {}", configs);
            environmentMetrics = new EnvironmentMetrics();
            jvmCollector = executor.scheduleAtFixedRate(this::collectJvmMetrics, frequencyInMs, frequencyInMs, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        metrics.forEach(this::metricChange);
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        synchronized (lock) {
            var metricName = metric.metricName();
            logger.debug("New metric: metricName={}", metricName);
            if (watched(metricName)) {
                logger.debug("Watched metric: metricName={}", metricName);
                metrics.put(new MetricKey(prefix, metricName.name(), metric.metricName().tags()), metric);
            }
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        synchronized (lock) {
            metrics.remove(new MetricKey(prefix, metric.metricName().name(), metric.metricName().tags()));
        }
    }

    @Override
    public void close() {
        logger.info("Closing metrics...");
        if (Objects.nonNull(mainCollector)) {
            mainCollector.cancel(false);
        }
        if (Objects.nonNull(jvmCollector)) {
            jvmCollector.cancel(false);
        }
        if (Objects.nonNull(executor)) {
            executor.shutdown();
        }
    }

    private boolean producerMetricsWatched(String metricName) {
        return switch (metricName) {
            case "batch-size-avg" -> true;
            case "buffer-available-bytes" -> true;
            case "compression-rate-avg" -> true;
            case "record-send-rate" -> true;
            case "record-send-total" -> true;
            case "record-size-avg" -> true;
            case "waiting-threads" -> true;
            default -> false;
        };
    }

    private boolean watched(MetricName metricName) {
        if (metricName.tags().containsKey("client-id") && 
            metricName.tags().get("client-id").endsWith("-restore-consumer")) {
            return false;
        }
        return switch (metricName) {

            // Consumer metrics
            case MetricName name when name.name().equals("records-lag") &&
                    name.group().equals("consumer-fetch-manager-metrics") ->
                true;

            case MetricName name when name.name().equals("records-consumed-rate") &&
                    name.group().equals("consumer-fetch-manager-metrics") &&
                    name.tags().containsKey("topic") ->
                true;

            case MetricName name when name.name().equals("assigned-partitions") &&
                    name.group().equals("consumer-coordinator-metrics") ->
                true;

            case MetricName name when name.name().equals("fetch-size-avg") &&
                    name.group().equals("consumer-fetch-manager-metrics") &&
                    name.tags().containsKey("topic") ->
                true;
            case MetricName name when name.name().equals("last-poll-seconds-ago") &&
                    name.group().equals("consumer-metrics") -> true;

            // Producer metrics
            case MetricName name when producerMetricsWatched(name.name()) &&
                    name.group().equals("producer-metrics") ->
                true;
            // Stream metrics
            case MetricName name when name.name().equals("alive-stream-threads")
                    && name.group().equals("stream-metrics") ->
                true;
            case MetricName name when name.name().equals("request-latency-avg") -> true;

            default -> false;
        };
    }

}
