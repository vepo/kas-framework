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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import dev.vepo.kafka.maestro.MaestroConfigs;

public abstract class AbstractMaestroMetrics implements MetricsReporter {
    private final Object lock;
    private final Map<MetricKey, KafkaMetric> metrics;
    private final ScheduledExecutorService executor;
    private String prefix;
    private EnvironmentMetrics environmentMetrics;
    private ScheduledFuture<?> scheduled;
    private final static Map<Class<? extends AbstractMaestroMetrics>, AtomicInteger> instanceCounter = new HashMap<>();

    protected AbstractMaestroMetrics(Object lock) {
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
        var frequencyInMs = mConfigs.getLong(MaestroConfigs.MAESTRO_METRICS_COLLECTOR_FREQUENCY_MS_CONFIG);
        synchronized (getClass()) {
            if (Objects.isNull(scheduled)) {
                // to avoid duplicated data only the first object will scrap JVM metrics
                var scrapJvmMetrics = instanceCounter.computeIfAbsent(getClass(), (_class) -> new AtomicInteger(0)).getAndIncrement() == 0;
                scheduled = executor.scheduleAtFixedRate(() -> {
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
                    if (scrapJvmMetrics) {
                        process(new PerformanceMetric("cpu-used", environmentMetrics.cpuUsed()));
                        process(new PerformanceMetric("cpu-total", environmentMetrics.cpuTotal()));
                        process(new PerformanceMetric("memory-total", environmentMetrics.memoryTotal()));
                        process(new PerformanceMetric("memory-used", environmentMetrics.memoryUsed()));
                    }
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
            if (instanceCounter.computeIfAbsent(getClass(), (_class) -> new AtomicInteger(0)).decrementAndGet() == 0) {
                instanceCounter.remove(getClass());
            }
        }
    }

    private boolean watched(KafkaMetric metric) {
        return metric.metricName().name().contains("records-lag") && metric.metricName().group().equals("consumer-fetch-manager-metrics");
    }
}
