package dev.vepo.kafka.maestro.adapter;

import dev.vepo.kafka.maestro.metrics.MetricListener;

public interface AdapterRule extends MetricListener {
    StreamsContext evaluate(StreamsContext context);
}
