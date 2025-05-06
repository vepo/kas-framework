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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.vepo.kafka.maestro.MaestroConfigs;

public abstract class MaestroMetrics implements MetricsReporter {
    private static final Logger logger = LoggerFactory.getLogger(MaestroMetrics.class);

    private final Object lock;
    private final Map<MetricKey, KafkaMetric> metrics;
    private final ScheduledExecutorService executor;
    private String prefix;
    private EnvironmentMetrics environmentMetrics;
    private ScheduledFuture<?> scheduled;

    protected MaestroMetrics(Object lock) {
        this.lock = lock;
        prefix = "";
        metrics = Collections.synchronizedMap(new HashMap<>());
        executor = Executors.newSingleThreadScheduledExecutor();
        this.environmentMetrics = new EnvironmentMetrics();
        this.scheduled = null;
    }

    protected abstract void process(PerformanceMetric metric);

    @Override
    public void configure(Map<String, ?> configs) {
        var mConfigs = new MaestroConfigs(configs);
        var frequencyInMs = mConfigs.getLong(MaestroConfigs.MAESTRO_METRICS_COLLECTION_FREQUENCY_MS_CONFIG);
        synchronized (lock) {
            if (Objects.isNull(scheduled)) {
                logger.info("Scheduling new collector task!");
                scheduled = executor.scheduleAtFixedRate(() -> {
                    metrics.forEach((key, value) -> {
                        if (value.metricValue() instanceof Number statValue && (!(statValue instanceof Double) || !Double.isNaN((double) statValue))) {
                            if (value.metricName().tags().containsKey("topic") && value.metricName().tags().containsKey("partition")) {
                                process(new PerformanceMetric(key.scope(),
                                                              key.name() + "-" + value.metricName().tags().get("topic") + "-"
                                                                      + value.metricName().tags().get("partition"),
                                                              value.metricName().tags().get("client-id"),
                                                              statValue,
                                                              System.currentTimeMillis()));
                            } else if (value.metricName().tags().containsKey("topic")) {
                                process(new PerformanceMetric(key.scope(),
                                                              key.name() + "-" + value.metricName().tags().get("topic"),
                                                              value.metricName().tags().get("client-id"),
                                                              statValue,
                                                              System.currentTimeMillis()));
                            } else {
                                process(new PerformanceMetric(key.scope(),
                                                              key.name(),
                                                              value.metricName().tags().get("client-id"),
                                                              statValue,
                                                              System.currentTimeMillis()));
                            }
                        }
                    });
                    logger.info("Collecting environment data!");
                    process(new PerformanceMetric("environment", "cpu-used", "jvm", environmentMetrics.cpuUsed(), System.currentTimeMillis()));
                    process(new PerformanceMetric("environment", "cpu-total", "jvm", environmentMetrics.cpuTotal(), System.currentTimeMillis()));
                    process(new PerformanceMetric("environment", "memory-total", "jvm", environmentMetrics.memoryTotal(), System.currentTimeMillis()));
                    process(new PerformanceMetric("environment", "memory-used", "jvm", environmentMetrics.memoryUsed(), System.currentTimeMillis()));
                }, frequencyInMs, frequencyInMs, TimeUnit.MILLISECONDS);
            }
        }
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
        if (Objects.nonNull(scheduled)) {
            scheduled.cancel(false);
        }
    }

    private boolean watched(KafkaMetric metric) {
        return metric.metricName().name().contains("records-lag") && metric.metricName().group().equals("consumer-fetch-manager-metrics");
    }
}
