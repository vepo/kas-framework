package io.vepo.kafka.stream.sample.metrics;

public record PerformanceMetric(String scope, String name, String clientId, Number value, long timestamp) {

}
