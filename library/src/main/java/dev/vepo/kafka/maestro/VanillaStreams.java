package dev.vepo.kafka.maestro;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.CloseOptions;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription.Source;

public class VanillaStreams implements Streams {

    private final KafkaStreams innerStreams;
    private Properties props;
    private final Supplier<Topology> topologyProvider;
    private Topology topology = null;
    private final AtomicReference<Set<String>> inputTopics;

    public VanillaStreams(Supplier<Topology> topologyProvider, Properties props) {
        this.topologyProvider = topologyProvider;
        this.inputTopics = new AtomicReference<>();
        this.props = props;
        this.topology = topologyProvider.get();
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
    public void addNewThreads(int requiredNumberOfThreads) {
        this.innerStreams.addStreamThread();
    }

    @Override
    public Set<String> inputTopics() {
        return inputTopics.updateAndGet(value -> {
            if (Objects.nonNull(value)) {
                return this.topology.describe()
                        .subtopologies()
                        .stream()
                        .flatMap(s -> s.nodes().stream())
                        .filter(n -> n instanceof Source)
                        .map(n -> (Source) n)
                        .flatMap(source -> source.topicSet().stream())
                        .collect(Collectors.toSet());
            }
            return value;
        });
    }

    @Override
    public Map<String, Object> originalConfigs() {
        return props.entrySet()
                    .stream()
                    .collect(Collectors.toMap(entry -> entry.getKey().toString(), Map.Entry::getValue));
    }
}
