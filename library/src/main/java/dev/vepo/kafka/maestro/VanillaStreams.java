package dev.vepo.kafka.maestro;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.CloseOptions;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.Topology;

public class VanillaStreams implements Streams {

    private final KafkaStreams innerStreams;
    private Properties props;

    public VanillaStreams(Topology topology, Properties props) {
        this.props = props;
        innerStreams = new KafkaStreams(topology, props);
    }

    @Override
    public void cleanUp() {
        this.innerStreams.cleanUp();
    }

    @Override
    public void start() {
        this.innerStreams.start();
    }

    @Override
    public void close() {
        this.innerStreams.close();
    }

    @Override
    public void setStateListener(StateListener listener) {
        this.innerStreams.setStateListener(listener);
    }

    @Override
    public int threadNumber() {
        return this.innerStreams.metadataForLocalThreads().size();
    }

    @Override
    public void close(CloseOptions options) {
        this.innerStreams.close(options);
    }

    @Override
    public void restart(Properties props) {
        // nothing
    }

    @Override
    public Map<String, Object> originalConfigs() {
        return props.entrySet()
                    .stream()
                    .collect(Collectors.toMap(entry -> entry.getKey().toString(), Map.Entry::getValue));
    }
}
