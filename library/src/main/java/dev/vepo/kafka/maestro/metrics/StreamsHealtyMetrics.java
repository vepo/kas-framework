package dev.vepo.kafka.maestro.metrics;

import static java.util.Objects.requireNonNull;

import java.util.Map;

import dev.vepo.kafka.maestro.MaestroConfigs;
import dev.vepo.kafka.maestro.adapter.Adapter;

public class StreamsHealtyMetrics extends AbstractMaestroMetrics {
    private static final Object LOCK = new Object();

    private Adapter adapter;
    
    public StreamsHealtyMetrics() {
        super(LOCK);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.adapter = (Adapter) requireNonNull(configs.get(MaestroConfigs.MAESTRO_ADAPTER_INSTANCE_CONFIG), "Adapter cannot be null!");
        super.configure(configs);
    }

    @Override
    protected void process(PerformanceMetric metric) {
        adapter.feed(metric);
    }
}
