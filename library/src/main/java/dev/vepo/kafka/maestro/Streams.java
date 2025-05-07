package dev.vepo.kafka.maestro;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams.CloseOptions;
import org.apache.kafka.streams.KafkaStreams.StateListener;

public interface Streams extends AutoCloseable {

    void cleanUp();

    void start();

    void close();

    void setStateListener(StateListener listener);

    int threadNumber();

    void close(CloseOptions options);

    void restart(Properties props);

    Map<String, Object> originalConfigs();
}
