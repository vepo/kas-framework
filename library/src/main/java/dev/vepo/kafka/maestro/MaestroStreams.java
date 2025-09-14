package dev.vepo.kafka.maestro;

import static dev.vepo.kafka.maestro.MaestroConfigs.MAESTRO_ADAPTER_CLASS_CONFIG;
import static dev.vepo.kafka.maestro.MaestroConfigs.MAESTRO_ADAPTER_INSTANCE_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.CloseOptions;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.vepo.kafka.maestro.adapter.Adapter;
import dev.vepo.kafka.maestro.metrics.StreamsHealthyMetrics;

public class MaestroStreams implements Streams {

    private static final Logger logger = LoggerFactory.getLogger(MaestroStreams.class);

    private static Properties setup(Properties props) {
        if (props.containsKey(METRIC_REPORTER_CLASSES_CONFIG)) {
            props.put(METRIC_REPORTER_CLASSES_CONFIG,
                    props.get(METRIC_REPORTER_CLASSES_CONFIG) + "," + StreamsHealthyMetrics.class.getName());
        } else {
            props.put(METRIC_REPORTER_CLASSES_CONFIG, StreamsHealthyMetrics.class.getName());
        }
        var configs = new MaestroConfigs(props);
        props.put(MAESTRO_ADAPTER_INSTANCE_CONFIG,
                configs.getConfiguredInstance(MAESTRO_ADAPTER_CLASS_CONFIG, Adapter.class));
        return props;
    }

    private KafkaStreams innerStreams;
    private Properties originalProps;
    private final Supplier<Topology> topologyProvider;
    private Topology topology = null;
    private StateListener stateListener;
    private final AtomicReference<Set<String>> inputTopics;
    private final ExecutorService threadPool;

    public MaestroStreams(Supplier<Topology> topologyProvider, Properties props) {
        this.originalProps = setup(props);
        this.stateListener = null;
        this.topologyProvider = topologyProvider;
        this.inputTopics = new AtomicReference<>();
        threadPool = Executors.newCachedThreadPool();
        this.topology = topologyProvider.get();
        this.innerStreams = new KafkaStreams(topology, this.originalProps);
        if (this.originalProps.get(MAESTRO_ADAPTER_INSTANCE_CONFIG) instanceof Adapter adapterResource) {
            this.stateListener = adapterResource;
            adapterResource.setup(this);
        }
        this.innerStreams.setStateListener(this.stateListener);
    }

    @Override
    public void close() {
        this.innerStreams.close();
        this.threadPool.shutdown();
        var adapter = this.originalProps.get(MAESTRO_ADAPTER_INSTANCE_CONFIG);
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
    public Set<String> inputTopics() {
        return inputTopics.updateAndGet(value -> {
            if (Objects.isNull(value)) {
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

    private static String threadDump(boolean lockedMonitors, boolean lockedSynchronizers) {
        StringBuffer threadDump = new StringBuffer(System.lineSeparator());
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        for (ThreadInfo threadInfo : threadMXBean.dumpAllThreads(lockedMonitors, lockedSynchronizers)) {
            threadDump.append(threadInfo.toString());
        }
        return threadDump.toString();
    }

    @Override
    public void restart(Properties props) {
        this.threadPool.submit(() -> {
            logger.info("Restarting Streams asynchronously... props={}", props);

            this.innerStreams.close(new CloseOptions().timeout(Duration.ofMinutes(1)).leaveGroup(false));

            if (this.innerStreams.state() != KafkaStreams.State.NOT_RUNNING
                    && this.innerStreams.state() != KafkaStreams.State.ERROR) {
                String dump = threadDump(true, true);
                logger.warn("Could not close streams, threads are still running:\n\n {}", dump);
            }

            logger.info("Streams stopped!");
            this.originalProps = props;
            this.topology = this.topologyProvider.get();
            this.innerStreams = new KafkaStreams(topology, props);
            this.innerStreams.setStateListener(this.stateListener);
            this.innerStreams.start();
        });
    }

    @Override
    public void addNewThreads(int requiredNumberOfThreads) {
        var currentThreadNumber = this.innerStreams.metadataForLocalThreads().size();
        Optional<String> threadName;
        do {
            threadName = this.innerStreams.addStreamThread();
            logger.info("Added new thread: {}", threadName);
            originalProps.compute(NUM_STREAM_THREADS_CONFIG,
                    (key, value) -> switch (value) {
                        case String sValue -> Integer.parseInt(sValue) + 1;
                        case Integer iValue -> iValue + 1;
                        case Number nValue -> nValue.intValue() + 1;
                        default -> 2;
                    });
            currentThreadNumber++;
        } while (threadName.isPresent() && currentThreadNumber < requiredNumberOfThreads);
    }

    @Override
    public Map<String, Object> originalConfigs() {
        return originalProps.entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> entry.getKey().toString(), Map.Entry::getValue));
    }
}