package dev.vepo.kafka.maestro.metrics;

public interface MetricListener extends AutoCloseable {
    void feed(PerformanceMetric metric);
}
