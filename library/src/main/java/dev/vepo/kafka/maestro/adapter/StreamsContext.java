package dev.vepo.kafka.maestro.adapter;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.kafka.streams.KafkaStreams;

import dev.vepo.kafka.maestro.Streams;
import dev.vepo.kafka.maestro.adapter.MetricKey.JvmMetricKey;
import dev.vepo.kafka.maestro.adapter.MetricKey.PartitionMetricKey;

public record StreamsContext(KafkaStreams.State streams, ThroughputState throughput, ResourcesState resources,
        Map<MetricKey, MetricValues> metrics, Streams instance) {

    public Stream<MetricValues> lagHistory() {
        return metrics.entrySet()
                .stream()
                .filter(e -> e.getKey() instanceof PartitionMetricKey pmk && pmk.name().equals("records-lag"))
                .map(Entry::getValue);
    }

    public double cpuUsage() {
        return metrics.entrySet()
                      .stream()
                      .filter(e -> e.getKey() instanceof JvmMetricKey pmk && pmk.name().equals("cpu-used"))
                      .map(Entry::getValue)
                      .mapToDouble(MetricValues::average)
                      .average()
                      .orElse(0.0);
    }

    public double memoryUsage() {
        return metrics.entrySet()
                      .stream()
                      .filter(e -> e.getKey() instanceof JvmMetricKey pmk && pmk.name().equals("memory-used"))
                      .map(Entry::getValue)
                      .mapToDouble(MetricValues::average)
                      .average()
                      .orElse(0.0);
    }

    public double cpuAvailable() {
        return metrics.entrySet()
                      .stream()
                      .filter(e -> e.getKey() instanceof JvmMetricKey pmk && pmk.name().equals("cpu-total"))
                      .map(Entry::getValue)
                      .mapToDouble(MetricValues::average)
                      .average()
                      .orElse(0.0);
    }

    public double memoryAvailable() {
        return metrics.entrySet()
                      .stream()
                      .filter(e -> e.getKey() instanceof JvmMetricKey pmk && pmk.name().equals("memory-total"))
                      .map(Entry::getValue)
                      .mapToDouble(MetricValues::average)
                      .average()
                      .orElse(0.0);
    }

    public int threadNumber() {
        if (Objects.isNull(instance)) {
            throw new IllegalStateException("Kafka Streams instance is not accessible!");
        }
        return instance.threadNumber();
    }

    public StreamsContext withStreams(KafkaStreams.State streams) {
        if (this.streams != streams) {
            return new StreamsContext(streams, throughput, resources, metrics, instance);
        } else {
            return this;
        }
    }

    public StreamsContext withThroughput(ThroughputState throughput) {
        if (this.throughput != throughput) {
            return new StreamsContext(streams, throughput, resources, metrics, instance);
        } else {
            return this;
        }
    }

    public StreamsContext withResources(ResourcesState resources) {
        if (this.resources != resources) {
            return new StreamsContext(streams, throughput, resources, metrics, instance);
        } else {
            return this;
        }
    }

    public StreamsContext withInstance(Streams instance) {
        return new StreamsContext(streams, throughput, resources, metrics, instance);
    }
}
