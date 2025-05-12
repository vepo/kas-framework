package dev.vepo.kafka.maestro.metrics;

import static java.util.Objects.requireNonNull;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.vepo.kafka.maestro.MaestroConfigs;
import dev.vepo.kafka.maestro.adapter.Adapter;

public class StreamsHealthyMetrics extends AbstractMaestroMetrics {
    private static final Logger logger = LoggerFactory.getLogger(StreamsHealthyMetrics.class);
    private static final Object LOCK = new Object();

    private Adapter adapter;
    
    public StreamsHealthyMetrics() {
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
