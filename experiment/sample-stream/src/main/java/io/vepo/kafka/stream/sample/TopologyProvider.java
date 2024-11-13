package io.vepo.kafka.stream.sample;

import java.time.Duration;
import java.util.Optional;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;

import io.vepo.kafka.stream.sample.serde.TrainInfoSerde;
import io.vepo.kafka.stream.sample.serde.TrainSpeedSerde;
import io.vepo.maestro.experiment.data.TrainMoviment;

public class TopologyProvider {

    public static Topology get(String pipeline, String testId) {
        var builder = new StreamsBuilder();
        switch (pipeline) {
            case "AGGREGATION":
                builder.<String, TrainMoviment>stream("train.moviment")
                       .groupByKey()
                       .windowedBy(TimeWindows.of(Duration.ofMinutes(2)))
                       .aggregate(
                                  () -> new TrainSpeed(0, 0),
                                  (key, value, aggregate) -> switch (value.eventType()) {
                                      case "DEPARTURE" -> new TrainSpeed(aggregate.departure() + 1, aggregate.arrival());
                                      case "ARRIVAL" -> new TrainSpeed(aggregate.departure(), aggregate.arrival() + 1);
                                      default -> aggregate;
                                  },
                                  Materialized.<String, TrainSpeed, WindowStore<Bytes, byte[]>>as("train-speed-store-" + testId)
                                              .withKeySerde(Serdes.String())
                                              .withValueSerde(new TrainSpeedSerde()))
                       .toStream()
                       // .peek((key, value) -> System.out.println("Key: " + key + " Value: " + value))
                       .to("train.event-summary-" + testId, Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), new TrainSpeedSerde()));
                break;
            case "TRANSFORM":
                builder.<String, TrainMoviment>stream("train.moviment")
                       .mapValues(data -> new TrainInfo(data.trainId(),
                                                        data.eventType(),
                                                        Optional.ofNullable(data.plannedTimestamp()).map(Long::valueOf).orElse(Long.valueOf(data.actualTimestamp())),
                                                        Long.valueOf(data.actualTimestamp()),
                                                        Long.valueOf(data.actualTimestamp()) - Optional.ofNullable(data.plannedTimestamp()).map(Long::valueOf).orElse(Long.valueOf(data.actualTimestamp()))))
                       .to("train.info-" + testId, Produced.with(Serdes.String(), new TrainInfoSerde()));
                break;
            default:
                throw new RuntimeException("Invalid pipeline! pipeline=" + System.getenv("PIPELINE"));

        }
        return builder.build();
    }

}
