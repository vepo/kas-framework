package dev.vepo.kafka.adaptive;

import static dev.vepo.kafka.adaptive.AdaptiveConfigs.ADAPTIVE_ADAPTER_CLASS_CONFIG;
import static dev.vepo.kafka.adaptive.AdaptiveConfigs.ADAPTIVE_ADAPTER_INSTANCE_CONFIG;
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

import dev.vepo.kafka.adaptive.adapter.Adapter;
import dev.vepo.kafka.adaptive.metrics.StreamsHealthyMetrics;

public class AdaptiveStreams implements Streams {

    private static final Logger logger = LoggerFactory.getLogger(AdaptiveStreams.class);

    private static Properties setup(Properties props) {
        if (props.containsKey(METRIC_REPORTER_CLASSES_CONFIG)) {
            props.put(METRIC_REPORTER_CLASSES_CONFIG, props.get(METRIC_REPORTER_CLASSES_CONFIG) + "," + StreamsHealthyMetrics.class.getName());
        } else {
            props.put(METRIC_REPORTER_CLASSES_CONFIG, StreamsHealthyMetrics.class.getName());
        }
        var configs = new AdaptiveConfigs(props);
        props.put(ADAPTIVE_ADAPTER_INSTANCE_CONFIG, configs.getConfiguredInstance(ADAPTIVE_ADAPTER_CLASS_CONFIG, Adapter.class));
        // props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG,
        // AdaptiveRocksDBConfigSetter.class);
        return props;
    }

    private KafkaStreams innerStreams;
    private Properties originalProps;
    private final Supplier<Topology> topologyProvider;
    private Topology topology = null;
    private StateListener stateListener;
    private final AtomicReference<Set<String>> inputTopics;
    private final ExecutorService threadPool;

    public AdaptiveStreams(Supplier<Topology> topologyProvider, Properties props) {
        this.originalProps = setup(props);
        this.stateListener = null;
        this.topologyProvider = topologyProvider;
        this.inputTopics = new AtomicReference<>();
        threadPool = Executors.newCachedThreadPool();
        this.topology = topologyProvider.get();
        this.innerStreams = new KafkaStreams(topology, this.originalProps);
        if (this.originalProps.get(ADAPTIVE_ADAPTER_INSTANCE_CONFIG) instanceof Adapter adapterResource) {
            this.stateListener = adapterResource;
            adapterResource.setup(this);
        }
        this.innerStreams.setStateListener(this.stateListener);
    }

    @Override
    public void close() {
        this.close(new CloseOptions());
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
        var adapter = this.originalProps.get(ADAPTIVE_ADAPTER_INSTANCE_CONFIG);
        if (Objects.nonNull(adapter) && adapter instanceof AutoCloseable adapterResource) {
            try {
                adapterResource.close();
            } catch (Exception e) {
                logger.warn("Could not close adapter", e);
            }
        }
        this.innerStreams.close();
        this.threadPool.shutdown();
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

            this.innerStreams.close(new CloseOptions().timeout(Duration.ofMinutes(3)).leaveGroup(false));

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