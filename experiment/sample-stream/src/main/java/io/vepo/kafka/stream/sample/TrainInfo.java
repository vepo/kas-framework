package io.vepo.kafka.stream.sample;

public record TrainInfo(String id, String event, long plannedTimestamp, long actualTimestamp, long delay) {
    
}
