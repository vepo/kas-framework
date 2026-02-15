package dev.vepo.kafka.adaptive.metrics;

import java.util.Map;

public record MetricKey(String scope, String name, Map<String, String> tags) {
    
}
