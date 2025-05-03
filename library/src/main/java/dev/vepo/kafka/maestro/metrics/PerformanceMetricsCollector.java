package dev.vepo.kafka.maestro.metrics;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.management.MBeanServerConnection;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.streams.errors.StreamsException;

public class PerformanceMetricsCollector implements MetricsReporter {
    record MetricKey(String scope, String name, Map<String, String> tags) {
    }

    private static final Object LOCK = new Object();

    private final Map<MetricKey, KafkaMetric> metrics;
    private final ScheduledExecutorService executor;
    private String prefix;
    private MBeanServerConnection mbsc;
    private OperatingSystemMXBean osMBean;
    private List<GarbageCollectorMXBean> gcMBean;
    private MetricsRecorder recorder;

    public PerformanceMetricsCollector() {
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
        this.recorder = null;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.recorder = new MetricsRecorder();
        var lastGarbageCollectorTime = new AtomicLong(gcMBean.stream()
                                                             .mapToLong(GarbageCollectorMXBean::getCollectionTime)
                                                             .sum());
        executor.scheduleAtFixedRate(() -> {
            metrics.forEach((key, value) -> {
                if (value.metricValue() instanceof Number statValue && (!(statValue instanceof Double) || !Double.isNaN((double) statValue))) {
                    if (value.metricName().tags().containsKey("topic")) {
                        recorder.feed(new PerformanceMetric(key.scope(),
                                                            key.name() + "-" + value.metricName().tags().get("topic"),
                                                            value.metricName().tags().get("client-id"),
                                                            statValue,
                                                            System.currentTimeMillis()));
                    } else {
                        recorder.feed(new PerformanceMetric(key.scope(),
                                                            key.name(),
                                                            value.metricName().tags().get("client-id"),
                                                            statValue,
                                                            System.currentTimeMillis()));
                    }
                }
            });
            recorder.feed(new PerformanceMetric("system", "cpu-load", "", osMBean.getSystemLoadAverage(), System.currentTimeMillis()));
            recorder.feed(new PerformanceMetric("system", "memory-free", "", Runtime.getRuntime().freeMemory(), System.currentTimeMillis()));
            var currentGarbageCollectorTime = gcMBean.stream().mapToLong(GarbageCollectorMXBean::getCollectionTime).sum();
            recorder.feed(new PerformanceMetric("jvm", "gc-time", "",
                                                currentGarbageCollectorTime - lastGarbageCollectorTime.getAndUpdate((old) -> currentGarbageCollectorTime),
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
            if (watched(metric)) {
                metrics.put(new MetricKey(prefix, metric.metricName().name(), metric.metricName().tags()), metric);
            }
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        synchronized (LOCK) {
            metrics.remove(new MetricKey(prefix, metric.metricName().name(), metric.metricName().tags()));
        }
    }

    @Override
    public void close() {
        if (Objects.nonNull(recorder)) {
            recorder.close();
        }
    }

    private boolean watched(KafkaMetric metric) {
        return metric.metricName().name().contains("records-lag");
    }

}
