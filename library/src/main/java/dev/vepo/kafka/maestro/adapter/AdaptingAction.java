package dev.vepo.kafka.maestro.adapter;

public interface AdaptingAction {
    StreamsContext execute(StreamsContext context);
}
