package dev.vepo.kafka.maestro.adapter;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.kafka.streams.KafkaStreams;

import dev.vepo.kafka.maestro.adapter.MetricKey.PartitionMetricKey;

public record StreamsContext(KafkaStreams.State streams, ThroughputState throughput, ResourcesState resources, Map<MetricKey, MetricValues> metrics) {

    public Stream<MetricValues> lagHistory() {
        return metrics.entrySet()
                      .stream()
                      .filter(e -> e.getKey() instanceof PartitionMetricKey pmk && pmk.name().equals("records-lag"))
                      .map(Entry::getValue);
    }

    public StreamsContext withStreams(KafkaStreams.State streams) {
        if (this.streams != streams) {
            return new StreamsContext(streams, throughput, resources, metrics);
        } else {
            return this;
        }
    }

    public StreamsContext withThroughput(ThroughputState throughput) {
        if (this.throughput != throughput) {
            return new StreamsContext(streams, throughput, resources, metrics);
        } else {
            return this;
        }
    }

}
