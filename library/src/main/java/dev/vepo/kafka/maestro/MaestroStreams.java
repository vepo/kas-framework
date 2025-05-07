package dev.vepo.kafka.maestro;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.KafkaStreams.CloseOptions;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.vepo.kafka.maestro.adapter.Adapter;
import dev.vepo.kafka.maestro.metrics.StreamsHealtyMetrics;

public class MaestroStreams implements Streams {

    private static final Logger logger = LoggerFactory.getLogger(MaestroStreams.class);

    private static Properties setup(Properties props) {
        if (props.containsKey(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG)) {
            props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG,
                    props.get(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG) + "," + StreamsHealtyMetrics.class.getName());
        } else {
            props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, StreamsHealtyMetrics.class.getName());
        }
        var configs = new MaestroConfigs(props);
        props.put(MaestroConfigs.MAESTRO_ADAPTER_INSTANCE_CONFIG, configs.getConfiguredInstance(MaestroConfigs.MAESTRO_ADAPTER_CLASS_CONFIG, Adapter.class));
        return props;
    }

    private KafkaStreams innerStreams;
    private Properties originalProps;
    private final Topology topology;
    private StateListener stateListener;

    public MaestroStreams(Topology topology, Properties props) {
        this.originalProps = setup(props);
        this.stateListener = null;
        this.topology = topology;
        this.innerStreams = new KafkaStreams(topology, this.originalProps);
        if (this.originalProps.get(MaestroConfigs.MAESTRO_ADAPTER_INSTANCE_CONFIG) instanceof Adapter adapterResource) {
            this.stateListener = adapterResource;
            adapterResource.setup(this);
        }
        this.innerStreams.setStateListener(this.stateListener);
    }

    @Override
    public void close() {
        this.innerStreams.close();
        var adapter = this.originalProps.get(MaestroConfigs.MAESTRO_ADAPTER_INSTANCE_CONFIG);
        if (Objects.nonNull(adapter) && adapter instanceof AutoCloseable adapterResource) {
            try {
                adapterResource.close();
            } catch (Exception e) {
                logger.warn("Could not close adapter", e);
            }
        }
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
    public int threadNumber() {
        return this.innerStreams.metadataForLocalThreads().size();
    }

    @Override
    public void setStateListener(StateListener listener) {
        this.stateListener = listener;
    }

    @Override
    public void close(CloseOptions options) {
        this.innerStreams.close(options);
    }

    @Override
    public void restart(Properties props) {
        this.innerStreams.close(new CloseOptions().timeout(Duration.ofMinutes(1)).leaveGroup(false));
        this.originalProps = props;
        this.innerStreams = new KafkaStreams(topology, props);
        this.innerStreams.setStateListener(this.stateListener);
        this.innerStreams.start();
    }

    @Override
    public Map<String, Object> originalConfigs() {
        return originalProps.entrySet()
                            .stream()
                            .collect(Collectors.toMap(entry -> entry.getKey().toString(), Map.Entry::getValue));
    }
}