package dev.vepo.kafka.adaptive.metrics;

import static java.util.Objects.requireNonNull;

import java.util.Map;

import dev.vepo.kafka.adaptive.AdaptiveConfigs;
import dev.vepo.kafka.adaptive.adapter.Adapter;

public class StreamsHealthyMetrics extends AbstractAdaptiveMetrics {

    private Adapter adapter;
    
    @Override
    public void configure(Map<String, ?> configs) {
        this.adapter = (Adapter) requireNonNull(configs.get(AdaptiveConfigs.ADAPTIVE_ADAPTER_INSTANCE_CONFIG), "Adapter cannot be null!");
        super.configure(configs);
    }

    @Override
    protected void process(PerformanceMetric metric) {
        adapter.feed(metric);
    }
}
