package dev.vepo.kafka.maestro.adapter;

import dev.vepo.kafka.maestro.metrics.PerformanceMetric;

public class ThroughputAnalyzerRule implements AdapterRule {

    @Override
    public void feed(PerformanceMetric metric) {
        
    }

    @Override
    public void close() throws Exception {
        
    }

    @Override
    public StreamsContext evaluate(StreamsContext context) {
        return context;
    }
    
}
