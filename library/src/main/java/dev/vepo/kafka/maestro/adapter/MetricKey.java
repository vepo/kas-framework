package dev.vepo.kafka.maestro.adapter;

public sealed interface MetricKey permits MetricKey.PartitionMetricKey, MetricKey.JvmMetricKey, MetricKey.ClientMetricKey {
    public final record PartitionMetricKey(String name, String topic, int partition) implements MetricKey {
    }

    public final record JvmMetricKey(String name) implements MetricKey {
    }

    public final record ClientMetricKey(String name) implements MetricKey {
    }

    public static MetricKey partition(String name, String topic, int partition) {
        return new PartitionMetricKey(name, topic, partition);
    }

    public static MetricKey jvm(String name) {
        return new JvmMetricKey(name);
    }

    public static MetricKey client(String name) {
        return new ClientMetricKey(name);
    }
}
