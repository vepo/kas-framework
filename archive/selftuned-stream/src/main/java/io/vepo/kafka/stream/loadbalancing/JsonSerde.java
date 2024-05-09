package io.vepo.kafka.stream.loadbalancing;

import java.util.Map;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.vepo.kafka.stream.loadbalancing.LoadBalancingStream.Flow;
import io.vepo.kafka.stream.loadbalancing.LoadBalancingStream.Observation;
import io.vepo.kafka.stream.loadbalancing.LoadBalancingStream.Speed;

public class JsonSerde implements Serde<Object> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonSerde.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<String, Class<?>> CLASS_MAP = Map.of("flow", Flow.class,
                                                                  "speed", Speed.class,
                                                                  "observation", Observation.class);

    @Override
    public Serializer<Object> serializer() {
        return (topic, data) -> {
            try {
                if (!CLASS_MAP.containsKey(topic)) {
                    return new StringBuilder(data.getClass().getName()).append("=")
                                                                       .append(MAPPER.writeValueAsString(data))
                                                                       .toString()
                                                                       .getBytes();
                } else {
                    return MAPPER.writeValueAsBytes(data);
                }
            } catch (Exception e) {
                throw new KafkaException(e);
            }
        };
    }

    @Override
    public Deserializer<Object> deserializer() {
        return (String topic, byte[] data) -> {
            try {
                if (!CLASS_MAP.containsKey(topic)) {
                    String[] parts = new String(data).split("=");
                    return MAPPER.readValue(parts[1], Class.forName(parts[0]));
                } else {
                    return MAPPER.readValue(data, CLASS_MAP.get(topic));
                }
            } catch (Exception e) {
                LOGGER.error("Could not deserialize data for topic: {} - {}", topic, new String(data));
                throw new KafkaException("Could not deserialize data for topic: " + topic, e);
            }
        };
    }

}
