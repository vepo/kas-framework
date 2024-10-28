package io.vepo.kafka.maestro.metrics;

import java.util.Map;

public record PerformanceMetric(String scope, String name, String clientId, Number value, long timestamp) {

}
