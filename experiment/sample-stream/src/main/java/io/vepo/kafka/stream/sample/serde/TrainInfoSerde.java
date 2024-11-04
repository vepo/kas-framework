package io.vepo.kafka.stream.sample.serde;

import java.io.IOException;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.StreamsException;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.vepo.kafka.stream.sample.SampleStream.TrainInfo;

public class TrainInfoSerde implements Serde<TrainInfo> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Serializer<TrainInfo> serializer() {
        return (topic, data) -> {
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new StreamsException("Could not serialize data for topic: " + topic, e);
            }
        };
    }

    @Override
    public Deserializer<TrainInfo> deserializer() {
        return (topic, data) -> {
            try {
                return MAPPER.readValue(data, TrainInfo.class);
            } catch (IOException e) {
                throw new StreamsException("Could not deserialize data for topic: " + topic, e);
            }
        };
    }

}
