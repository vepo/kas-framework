package io.vepo.kafka.stream.sample;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import io.vepo.kafka.maestro.MaestroStream;
import io.vepo.kafka.stream.sample.serde.JsonSerde;
import io.vepo.maestro.experiment.data.TrainMoviment;

public class SampleStream {

    public record TrainSpeed(int departure, int arrival) {
    }

    public static void main(String[] args) {
        var builder = new StreamsBuilder();
        builder.<String, TrainMoviment>stream("train.moviment")
               .groupByKey()
               .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)))
               .aggregate(() -> new TrainSpeed(0, 0),
                          (key, value, aggregate) -> switch (value.eventType()) {
                              case "DEPARTURE" -> new TrainSpeed(aggregate.departure() + 1, aggregate.arrival());
                              case "ARRIVAL" -> new TrainSpeed(aggregate.departure(), aggregate.arrival() + 1);
                              default -> aggregate;
                          },
                          (key, v1, v2) -> new TrainSpeed(v1.departure() + v2.departure(), v1.arrival() + v2.arrival()))
               .toStream()
               .to("train.event-summary", Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class)));

        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0:9092,kafka-1:9094,kafka-2:9096");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "load-balancing-0");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        var maestroStream = MaestroStream.create(builder.build(), props);
        maestroStream.start();
    }
}
