package dev.vepo.kafka.maestro.metrics;

public record PerformanceMetric(String scope, String id, Context context, String name,
                                String clientId, Number value, String topic, int partition,
                                long timestamp) {
    public enum Context {
        PARTITION,
        TOPIC,
        CLIENT,
        JVM,
        BROKER
    };

    public PerformanceMetric(Context context, String name, String clientId, Number value) {
        this(context.name().toLowerCase(), name, context, name, clientId, value, "", -1, System.currentTimeMillis());
    }

    public PerformanceMetric(String scope, String name, String clientId, Number value) {
        this(scope, String.format("%s-%s", name, clientId), Context.CLIENT, name, clientId, value, "", -1, System.currentTimeMillis());
    }

    public PerformanceMetric(String scope, String name, String clientId, Number value, String topic) {
        this(scope, String.format("%s-%s-%s", name, topic, clientId), Context.TOPIC, name, clientId, value, topic, -1, System.currentTimeMillis());
    }

    public PerformanceMetric(String scope, String name, String clientId, Number value, String topic, int partition) {
        this(scope, String.format("%s-%s-%d", name, topic, partition), Context.PARTITION, name, clientId, value, topic, partition, System.currentTimeMillis());
    }
}
