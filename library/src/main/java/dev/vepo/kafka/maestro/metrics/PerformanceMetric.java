package dev.vepo.kafka.maestro.metrics;

public record PerformanceMetric(String scope, String name, String clientId, Number value, long timestamp) {

}
