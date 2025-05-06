package dev.vepo.kafka.maestro.adapter;

public enum StreamsStatus {
    NOT_STARTED,
    INITIALIZING,
    RUNNING,
    SUSTAINABLE,
    UNSUSTAINABLE, 
    ERROR
}
