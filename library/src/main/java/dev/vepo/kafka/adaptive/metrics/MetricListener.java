package dev.vepo.kafka.adaptive.metrics;

public interface MetricListener extends AutoCloseable {
    void feed(PerformanceMetric metric);
}
