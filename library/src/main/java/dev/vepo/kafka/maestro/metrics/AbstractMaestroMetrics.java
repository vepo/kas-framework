package dev.vepo.kafka.maestro.metrics;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.streams.StreamsConfig;

import dev.vepo.kafka.maestro.MaestroConfigs;

public abstract class AbstractMaestroMetrics implements MetricsReporter {
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

    private void startJvmMetricsCollector(Map<String, ?> configs, Long frequencyInMs) {
        // Check if the client is the Streams, not the producer, consumer or admin
        // client
        if (configs.containsKey(StreamsConfig.APPLICATION_ID_CONFIG) && configs.containsKey(StreamsConfig.CLIENT_ID_CONFIG)) {
            var clientId = (String) configs.get(StreamsConfig.CLIENT_ID_CONFIG);
            if (Objects.nonNull(clientId) && !clientId.endsWith("-admin") && !clientId.endsWith("-producer") && !clientId.endsWith("-admin")) {
                this.environmentMetrics = new EnvironmentMetrics();
                jvmCollector = executor.scheduleAtFixedRate(this::collectJvmMetrics, frequencyInMs, frequencyInMs, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void collectJvmMetrics() {
        process(new PerformanceMetric("cpu-used", environmentMetrics.cpuUsed()));
        process(new PerformanceMetric("cpu-total", environmentMetrics.cpuTotal()));
        process(new PerformanceMetric("memory-total", environmentMetrics.memoryTotal()));
        process(new PerformanceMetric("memory-used", environmentMetrics.memoryUsed()));
    }

    private void collectMainMetrics() {
        metrics.forEach((key, value) -> {
            if (value.metricValue() instanceof Number statValue && (!(statValue instanceof Double) || !Double.isNaN((double) statValue))) {
                if (value.metricName().tags().containsKey("topic") && value.metricName().tags().containsKey("partition")) {
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
        startJvmMetricsCollector(configs, frequencyInMs);
        mainCollector = executor.scheduleAtFixedRate(this::collectMainMetrics, frequencyInMs, frequencyInMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        metrics.forEach(this::metricChange);
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        synchronized (lock) {
            if (watched(metric)) {
                metrics.put(new MetricKey(prefix, metric.metricName().name(), metric.metricName().tags()), metric);
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
        if (Objects.nonNull(mainCollector)) {
            mainCollector.cancel(false);
        }
        if (Objects.nonNull(jvmCollector)) {
            jvmCollector.cancel(false);
        }
    }

    private boolean watched(KafkaMetric metric) {
        return (metric.metricName().name().contains("records-lag") && metric.metricName().group().equals("consumer-fetch-manager-metrics")) ||
                (metric.metricName().name().equals("alive-stream-threads") && metric.metricName().group().equals("stream-metrics"));
    }
}
