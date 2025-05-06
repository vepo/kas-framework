package dev.vepo.kafka.maestro.adapter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.vepo.kafka.maestro.MaestroConfigs;
import dev.vepo.kafka.maestro.metrics.MetricListener;
import dev.vepo.kafka.maestro.metrics.PerformanceMetric;

public class Adapter implements MetricListener, Configurable {

    private static final Logger logger = LoggerFactory.getLogger(Adapter.class);

    private final ScheduledExecutorService taskExecutor;
    private KafkaStreams streams;
    private StreamsContext context;
    private final List<AdapterRule> rules;

    public Adapter() {
        this.taskExecutor = Executors.newSingleThreadScheduledExecutor();
        this.context = new StreamsContext(StreamsStatus.NOT_STARTED);
        this.rules = List.of(new ThroughputAnalyzerRule(), new ThreadAllocationRule());
    }

    @Override
    public void close() throws Exception {
        taskExecutor.shutdown();
        if (!taskExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
            taskExecutor.shutdownNow();
        }
        taskExecutor.close();
    }

    @Override
    public void configure(Map<String, ?> props) {
        var configs = new MaestroConfigs(props);
        var frequencyMs = configs.getLong(MaestroConfigs.MAESTRO_ADAPTER_FREQUENCY_MS_CONFIG);
        taskExecutor.scheduleAtFixedRate(this::verify, frequencyMs, frequencyMs, TimeUnit.MILLISECONDS);
    }

    public void setup(KafkaStreams streams) {
        this.streams = streams;
        this.streams.setStateListener(this::kafkaStreamsStateChanged);
    }

    private synchronized void kafkaStreamsStateChanged(KafkaStreams.State newState, KafkaStreams.State oldState) {
        if (oldState == KafkaStreams.State.REBALANCING && newState == KafkaStreams.State.RUNNING) {
            this.context = new StreamsContext(StreamsStatus.RUNNING);
        } else if (oldState == KafkaStreams.State.RUNNING && newState == KafkaStreams.State.PENDING_ERROR) {
            this.context = new StreamsContext(StreamsStatus.ERROR);
        }
    }

    @Override
    public void feed(PerformanceMetric metric) {
        rules.forEach(r -> r.feed(metric));
    }

    private synchronized void verify() {
        switch (context.status()) {
            case NOT_STARTED:
            case INITIALIZING:
                // nothing
                break;
            case RUNNING:
                this.context = rules.stream()
                                    .sequential()
                                    .reduce(this.context, // Initial value
                                            (ctx, rule) -> rule.evaluate(ctx), // Accumulator
                                            (ctx1, ctx2) -> ctx2 // Combiner (not used in sequential streams)
                                    );
            case ERROR:
                // nothing
                break;
            default:
                // nothing to do!
        }
    }
}