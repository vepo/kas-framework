package io.vepo.kafka.maestro.metrics;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServerConnection;

import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vepo.kafka.maestro.MaestroConfigs;

public class MaestroMetricsCollector implements MetricsReporter, ClusterResourceListener {

    private static final Logger logger = LoggerFactory.getLogger(MaestroMetricsCollector.class);

    record MetricKey(String scope, String name, Map<String, String> tags) {
    }

    private final Map<MetricKey, KafkaMetric> metrics;
    private final ScheduledExecutorService executor;
    private static final Object LOCK = new Object();
    private String prefix;
    private MBeanServerConnection mbsc;
    private OperatingSystemMXBean osMBean;
    private List<GarbageCollectorMXBean> gcMBean;

    public MaestroMetricsCollector() {
        prefix = "";
        metrics = Collections.synchronizedMap(new HashMap<>());
        executor = Executors.newSingleThreadScheduledExecutor();
        try {
            mbsc = ManagementFactory.getPlatformMBeanServer();
            osMBean = ManagementFactory.newPlatformMXBeanProxy(mbsc, ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME, OperatingSystemMXBean.class);
            gcMBean = ManagementFactory.getGarbageCollectorMXBeans();
        } catch (IOException e) {
            throw new StreamsException(e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        if (configs.containsKey(MaestroConfigs.MAESTRO_PERFORMANCE_OPTIMIZER_CONFIG) &&
                configs.get(MaestroConfigs.MAESTRO_PERFORMANCE_OPTIMIZER_CONFIG) instanceof PerformanceOptimizer optimizer) {
            AtomicLong lastGarbageCollectorTime = new AtomicLong(gcMBean.stream().mapToLong(GarbageCollectorMXBean::getCollectionTime).sum());
            executor.scheduleAtFixedRate(() -> {
                var metricNames = metrics.keySet()
                                         .stream()
                                         .map(MetricKey::name)
                                         .collect(Collectors.toSet());
                
                metricNames.forEach(name -> {
                    var currMetrics = metrics.entrySet()
                                             .stream()
                                             .filter(e -> e.getKey().name().equals(name))
                                             .toList();
                    if (!currMetrics.isEmpty()) {
                        var first = currMetrics.get(0);
                        if (first.getValue().metricValue() instanceof Number number) {
                            Number avgValue = number;
                            if (number instanceof Integer) {
                                avgValue = currMetrics.stream().mapToInt(e -> (int) e.getValue().metricValue()).average().orElse((int) avgValue);
                            } else if (number instanceof Double) {
                                avgValue = currMetrics.stream().mapToDouble(e -> (double) e.getValue().metricValue()).average().orElse((double) avgValue);
                            } else if (number instanceof Long) {
                                avgValue = currMetrics.stream().mapToLong(e -> (long) e.getValue().metricValue()).average().orElse((long) avgValue);
                            }
                            optimizer.feed(new PerformanceMetric(first.getKey().scope(),
                                                                 first.getKey().name(),
                                                                 first.getValue().metricName().tags().get("client-id"),
                                                                 avgValue,
                                                                 System.currentTimeMillis()));
                        }
                    }
                });
                optimizer.feed(new PerformanceMetric("system", "cpu-load", "", osMBean.getSystemLoadAverage(), System.currentTimeMillis()));
                optimizer.feed(new PerformanceMetric("system", "memory-free", "", Runtime.getRuntime().freeMemory(), System.currentTimeMillis()));
                var currentGarbageCollectorTime = gcMBean.stream().mapToLong(GarbageCollectorMXBean::getCollectionTime).sum();
                optimizer.feed(new PerformanceMetric("jvm", "gc-time", "", currentGarbageCollectorTime - lastGarbageCollectorTime.getAndUpdate((old) -> currentGarbageCollectorTime), System.currentTimeMillis()));

            }, 10, 10, TimeUnit.SECONDS);
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
            // logger.info("Metric: {} producer? = {} consumer? = {}", metric.metricName(), isProducerRelevante(metric), isConsumerRelevante(metric));
            if (isNotRestoreConsumer(metric)) {
                metrics.put(new MetricKey(prefix, metric.metricName().name(), metric.metricName().tags()), metric);
            }
        }
    }

    private boolean isProducerRelevante(KafkaMetric metric) {
        return metric.metricName().tags().get("topic") != null &&
                metric.metricName().tags().get("topic").startsWith("train.event-summary-");
    }

    private boolean isConsumerRelevante(KafkaMetric metric) {
        return metric.metricName().tags().get("topic") != null &&
                metric.metricName().tags().get("topic").equals("train.moviment");
    }

    private boolean isNotRestoreConsumer(KafkaMetric metric) {
        return !metric.metricName().tags().containsKey("client-id") ||
                !metric.metricName().tags().get("client-id").contains("-restore-consumer");
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        synchronized (LOCK) {
            metrics.remove(new MetricKey(prefix, metric.metricName().name(), metric.metricName().tags()));
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
