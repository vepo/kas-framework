package io.vepo.kafka.stream.sample.serde;

import java.io.IOException;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.StreamsException;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.vepo.kafka.stream.sample.SampleStream.TrainSpeed;

public class TrainSpeedSerde implements Serde<TrainSpeed> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Serializer<TrainSpeed> serializer() {
        return (topic, data) -> {
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new StreamsException("Could not serialize data for topic: " + topic, e);
            }
        };
    }

    @Override
    public Deserializer<TrainSpeed> deserializer() {
        return (topic, data) -> {
            try {
                return MAPPER.readValue(data, TrainSpeed.class);
            } catch (IOException e) {
                throw new StreamsException("Could not deserialize data for topic: " + topic, e);
            }
        };
    }

}
