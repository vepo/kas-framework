package dev.vepo.kafka.maestro.adapter;

public interface AdapterRule {
    StreamsContext evaluate(StreamsContext context);
}
