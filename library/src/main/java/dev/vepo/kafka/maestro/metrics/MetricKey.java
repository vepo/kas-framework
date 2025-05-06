package dev.vepo.kafka.maestro.metrics;

import java.util.Map;

public record MetricKey(String scope, String name, Map<String, String> tags) {
    
}
