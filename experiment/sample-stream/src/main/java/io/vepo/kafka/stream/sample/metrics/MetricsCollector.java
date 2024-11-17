package io.vepo.kafka.stream.sample.metrics;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsCollector implements MetricsReporter, ClusterResourceListener {
    private static final Logger logger = LoggerFactory.getLogger(MetricsCollector.class);

    record MetricKey(String scope, String name, Map<String, String> tags) {
    }

    private final Map<MetricKey, KafkaMetric> metrics;
    private final ScheduledExecutorService executor;
    private static final Object LOCK = new Object();
    private String prefix;
    private final MetricWriter writer;
    private MemoryMXBean memoryMXBean;
    private MBeanServer mbs;

    public MetricsCollector() {
        prefix = "";
        metrics = Collections.synchronizedMap(new HashMap<>());
        executor = Executors.newSingleThreadScheduledExecutor();
        writer = new MetricWriter();
        memoryMXBean = ManagementFactory.getMemoryMXBean();
        mbs = ManagementFactory.getPlatformMBeanServer();
    }

    public double getProcessCpuLoad() {
        try {
            ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
            AttributeList list = mbs.getAttributes(name, new String[] { "ProcessCpuLoad" });

            if (list.isEmpty()) {
                return 0.0;
            }
            Attribute att = (Attribute) list.get(0);
            Double value = (Double) att.getValue();

            if (value < 0.0) {
                return 0.0; // usually takes a couple of seconds before we get real values
            }
            return ((int) (value * 1000) / 10.0); // returns a percentage value with 1 decimal point precision
        } catch (Exception ex) {
            return 0.0;
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
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
                            avgValue = currMetrics.stream().mapToInt(e -> (int) e.getValue().metricValue())
                                    .average().orElse((int) avgValue);
                        } else if (number instanceof Double) {
                            avgValue = currMetrics.stream().mapToDouble(e -> (double) e.getValue().metricValue())
                                    .average().orElse((double) avgValue);
                        } else if (number instanceof Long) {
                            avgValue = currMetrics.stream().mapToLong(e -> (long) e.getValue().metricValue())
                                    .average().orElse((long) avgValue);
                        }
                        writer.write(new PerformanceMetric(first.getKey().scope(),
                                first.getKey().name(),
                                first.getValue().metricName().tags().get("client-id"),
                                avgValue,
                                System.currentTimeMillis()));
                    }
                }
            });
            var cpuLoad = getProcessCpuLoad();
            if (cpuLoad > 0) {
                writer.write(new PerformanceMetric("system", "cpu-load", "", cpuLoad, System.currentTimeMillis()));
            }
            writer.write(new PerformanceMetric("system", "memory-free", "", Runtime.getRuntime().freeMemory(),
                    System.currentTimeMillis()));
            writer.write(
                    new PerformanceMetric("system", "memory-usage", "", memoryMXBean.getHeapMemoryUsage().getUsed(),
                            System.currentTimeMillis()));
        }, 10, 10, TimeUnit.SECONDS);
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        metrics.forEach(this::metricChange);
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        synchronized (LOCK) {
            // logger.info("Metric: {} producer? = {} consumer? = {}", metric.metricName(),
            // isProducerRelevante(metric), isConsumerRelevante(metric));
            if (isNotRestoreConsumer(metric)) {
                metrics.put(new MetricKey(prefix, metric.metricName().name(), metric.metricName().tags()), metric);
            }
        }
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
