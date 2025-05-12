package dev.vepo.kafka.maestro.adapter.context;

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.kafka.streams.KafkaStreams;

import dev.vepo.kafka.maestro.Streams;
import dev.vepo.kafka.maestro.adapter.stats.StatsKey;
import dev.vepo.kafka.maestro.adapter.stats.StatsKey.BrokerStatsKey;
import dev.vepo.kafka.maestro.adapter.stats.StatsKey.ClientStatsKey;
import dev.vepo.kafka.maestro.adapter.stats.StatsKey.JvmStatsKey;
import dev.vepo.kafka.maestro.adapter.stats.StatsKey.PartitionStatsKey;
import dev.vepo.kafka.maestro.adapter.stats.StatsKey.TopicStatsKey;
import dev.vepo.kafka.maestro.adapter.stats.StatsValues;

public record StreamsContext(KafkaStreams.State streams, ThroughputState throughput, ResourcesState resources,
        Map<StatsKey, StatsValues> stats, Streams instance) {

    public Stream<StatsValues> lagHistory() {
        return stats.entrySet()
                .stream()
                .filter(e -> e.getKey() instanceof PartitionStatsKey psk && psk.name().equals("records-lag"))
                .map(Entry::getValue);
    }

    public Double accumulatedLag() {
        return stats.entrySet()
                    .stream()
                    .filter(e -> e.getKey() instanceof PartitionStatsKey psk && psk.name().equals("records-lag"))
                    .filter(e -> e.getKey() instanceof PartitionStatsKey psk && !psk.topic().contains("-changelog"))
                    .map(Entry::getValue)
                    .mapToDouble(StatsValues::last)
                    .sum();
    }


    public Double accumulatedThroughput() {
        return stats.entrySet()
                    .stream()
                    .filter(e -> e.getKey() instanceof TopicStatsKey tsk && tsk.name().equals("records-consumed-rate"))
                    .map(Entry::getValue)
                    .mapToDouble(StatsValues::last)
                    .sum();
    }

    private void clearClientMetrics() {
        stats.entrySet()
               .stream()
               .filter(e -> e.getKey() instanceof PartitionStatsKey || 
                            e.getKey() instanceof JvmStatsKey || 
                            e.getKey() instanceof ClientStatsKey ||
                            (e.getKey() instanceof TopicStatsKey tsk && !tsk.broker()))
               .map(Entry::getValue)
               .forEach(StatsValues::clear);

    }

    public double cpuUsage() {
        return stats.entrySet()
                      .stream()
                      .filter(e -> e.getKey() instanceof JvmStatsKey psk && psk.name().equals("cpu-used"))
                      .map(Entry::getValue)
                      .peek(value -> System.out.println("CPU Usage: " + value.all()))
                      .mapToDouble(StatsValues::average)
                      .peek(value -> System.out.println("CPU Usage: " + value))
                      .average()
                      .orElse(0.0);
    }

    public double memoryUsage() {
        return stats.entrySet()
                      .stream()
                      .filter(e -> e.getKey() instanceof JvmStatsKey pmk && pmk.name().equals("memory-used"))
                      .map(Entry::getValue)
                      .mapToDouble(StatsValues::average)
                      .average()
                      .orElse(0.0);
    }

    public double cpuAvailable() {
        return stats.entrySet()
                      .stream()
                      .filter(e -> e.getKey() instanceof JvmStatsKey pmk && pmk.name().equals("cpu-total"))
                      .map(Entry::getValue)
                      .mapToDouble(StatsValues::average)
                      .average()
                      .orElse(0.0);
    }

    public double memoryAvailable() {
        return stats.entrySet()
                      .stream()
                      .filter(e -> e.getKey() instanceof JvmStatsKey pmk && pmk.name().equals("memory-total"))
                      .map(Entry::getValue)
                      .mapToDouble(StatsValues::average)
                      .average()
                      .orElse(0.0);
    }

    public int assignedPartitions() {
        return (int) stats.entrySet()
                    .stream()
                    .filter(e -> e.getKey() instanceof ClientStatsKey psk && psk.name().equals("assigned-partitions"))
                    .map(Entry::getValue)
                    .mapToInt(stats -> stats.last().intValue())
                    .average()
                    .orElse(1.0);
    }

    public int totalPartitions() {
        return stats.entrySet()
                    .stream()
                    .filter(e -> e.getKey() instanceof TopicStatsKey tsk && tsk.name().equals("partitions-count"))
                    .map(Entry::getValue)
                    .mapToInt(stats -> stats.last().intValue())
                    .sum();
    }

    public int numBrokers() {
        return stats.entrySet()
                    .stream()
                    .filter(e -> e.getKey() instanceof BrokerStatsKey bsk && bsk.name().equals("brokers-count"))
                    .map(Entry::getValue)
                    .mapToInt(stats -> stats.last().intValue())
                    .sum();
    }

    public int threadNumber() {
        if (Objects.isNull(instance)) {
            throw new IllegalStateException("Kafka Streams instance is not accessible!");
        }
        return instance.threadNumber();
    }

    public StreamsContext withStreams(KafkaStreams.State streams) {
        if (this.streams != streams) {
            return new StreamsContext(streams, throughput, resources, stats, instance);
        } else {
            return this;
        }
    }

    public StreamsContext withThroughput(ThroughputState throughput) {
        if (this.throughput != throughput) {
            return new StreamsContext(streams, throughput, resources, stats, instance);
        } else {
            return this;
        }
    }

    public StreamsContext withResources(ResourcesState resources) {
        if (this.resources != resources) {
            return new StreamsContext(streams, throughput, resources, stats, instance);
        } else {
            return this;
        }
    }

    public StreamsContext withInstance(Streams instance) {
        return new StreamsContext(streams, throughput, resources, stats, instance);
    }



    public StreamsContext reset() {
        clearClientMetrics();
        return new StreamsContext(streams, ThroughputState.INITIALIZING, ResourcesState.INITIALIZING, stats, instance);
    }
}
