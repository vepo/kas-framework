package dev.vepo.maestro.experiment.stream.serdes;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

public class JsonSerde<T> implements Serde<T> {

    private class ObjectSerializer implements Serializer<T> {

        @Override
        public byte[] serialize(String topic, T data) {
            try {
                return writer.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new KafkaException("Cannot serialize data!", e);
            }
        }

    }

    private class ObjectDeserializer implements Deserializer<T> {

        @Override
        public T deserialize(String topic, byte[] data) {
            try {
                return reader.readValue(new ByteArrayInputStream(data));
            } catch (IOException e) {
                throw new KafkaException("Cannot deserialize data!", e);
            }
        }

    }

    public static <T> JsonSerde<T> of(Class<T> serderClass) {
        return new JsonSerde<>(serderClass);
    }
    private final ObjectReader reader;
    private final ObjectWriter writer;
    private final JsonSerde<T>.ObjectSerializer serializer;

    private final JsonSerde<T>.ObjectDeserializer deserializer;

    public JsonSerde(Class<T> serderClass) {
        var mapper = new ObjectMapper();
        this.reader = mapper.readerFor(serderClass);
        this.writer = mapper.writerFor(serderClass);
        this.serializer = new ObjectSerializer();
        this.deserializer = new ObjectDeserializer();
    }

    @Override
    public Serializer<T> serializer() {
        return this.serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this.deserializer;
    }

}
