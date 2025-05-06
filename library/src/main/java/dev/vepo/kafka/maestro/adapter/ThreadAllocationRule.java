package dev.vepo.kafka.maestro.adapter;

public class ThreadAllocationRule implements AdapterRule {

    @Override
    public StreamsContext evaluate(StreamsContext context) {
        return context;
    }
}
