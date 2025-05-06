package dev.vepo.kafka.maestro.adapter;

public interface MetricKey {
    public record PartitionMetricKey(String name, String topic, int partition) implements MetricKey {

    }

    public static MetricKey partition(String name, String topic, int partition) {
        return new PartitionMetricKey(name, topic, partition);
    }
}
