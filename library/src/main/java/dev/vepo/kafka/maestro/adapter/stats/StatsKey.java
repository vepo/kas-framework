package dev.vepo.kafka.maestro.adapter.stats;

public sealed interface StatsKey permits StatsKey.PartitionStatsKey, 
                                          StatsKey.JvmStatsKey, 
                                          StatsKey.ClientStatsKey, 
                                          StatsKey.BrokerStatsKey, 
                                          StatsKey.TopicStatsKey {
    public final record PartitionStatsKey(String name, String topic, int partition) implements StatsKey {
    }

    public final record JvmStatsKey(String name) implements StatsKey {
    }

    public final record BrokerStatsKey(String name) implements StatsKey {
    }

    public final record TopicStatsKey(String name, String topic, boolean broker, String clientId) implements StatsKey {
    }

    public final record ClientStatsKey(String name, String clientId) implements StatsKey {
    }

    public static StatsKey partition(String name, String topic, int partition) {
        return new PartitionStatsKey(name, topic, partition);
    }

    public static StatsKey jvm(String name) {
        return new JvmStatsKey(name);
    }

    public static StatsKey broker(String name) {
        return new BrokerStatsKey(name);
    }

    public static StatsKey topic(String name, String topic, boolean broker, String clientId) {
        return new TopicStatsKey(name, topic, broker, clientId);
    }

    public static StatsKey client(String name, String clientId) {
        return new ClientStatsKey(name, clientId);
    }
}
