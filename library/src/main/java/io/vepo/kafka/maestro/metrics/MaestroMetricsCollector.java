package io.vepo.kafka.maestro.metrics;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.streams.errors.StreamsException;

import io.vepo.kafka.maestro.MaestroConfigs;

public class MaestroMetricsCollector implements MetricsReporter, ClusterResourceListener {

    record MetricKey(String scope, String name) {
    }

    private final Map<MetricKey, KafkaMetric> metrics;
    private final ScheduledExecutorService executor;
    private static final Object LOCK = new Object();
    private String prefix;

    public MaestroMetricsCollector() {
        prefix = "";
        metrics = Collections.synchronizedMap(new HashMap<>());
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        if (configs.containsKey(MaestroConfigs.MAESTRO_PERFORMANCE_OPTIMIZER_CONFIG) &&
                configs.get(MaestroConfigs.MAESTRO_PERFORMANCE_OPTIMIZER_CONFIG) instanceof PerformanceOptimizer optimizer) {
            executor.scheduleAtFixedRate(() -> metrics.forEach((k, v) -> {
                if (v.metricValue() instanceof Number number) {
                    optimizer.feed(new PerformanceMetric(k.scope(),
                                                         k.name(),
                                                         v.metricName().tags(),
                                                         number,
                                                         System.currentTimeMillis()));
                }
            }), 10, 10, TimeUnit.SECONDS);
        } else {
            throw new StreamsException(String.format("No performance history configured! configs:%s", configs));
        }
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        metrics.forEach(this::metricChange);
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        synchronized (LOCK) {
            metrics.put(new MetricKey(prefix, metric.metricName().name()), metric);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        synchronized (LOCK) {
            metrics.remove(new MetricKey(prefix, metric.metricName().name()));
        }
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    @Override
    public void onUpdate(ClusterResource clusterResource) {
        // System.out.println("Cluster resource update: " + clusterResource);
    }

    @Override
    public void contextChange(MetricsContext metricsContext) {
        String namespace = metricsContext.contextLabels().get(MetricsContext.NAMESPACE);
        Objects.requireNonNull(namespace);
        synchronized (LOCK) {
            if (namespace.isEmpty()) {
                return;
            }

            prefix = namespace;
        }
    }

}
