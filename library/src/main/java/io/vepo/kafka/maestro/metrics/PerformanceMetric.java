package io.vepo.kafka.maestro.metrics;

import java.util.Map;

public record PerformanceMetric(String scope, String name, Map<String, String> tags, Number value, long timestamp) {

}
