package dev.vepo.kafka.maestro;

import static org.apache.kafka.streams.StreamsConfig.CLIENT_ID_CONFIG;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.streams.StreamsConfig;

import dev.vepo.kafka.maestro.adapter.Adapter;

public class MaestroConfigs extends AbstractConfig {
    public static final String MAESTRO_ADAPTER_INSTANCE_CONFIG = "maestro.adapter.instance";
    public static final String MAESTRO_ADAPTER_INSTANCE_DOC = "";

    public static final String MAESTRO_ADAPTER_CLASS_CONFIG = "maestro.adapter.class";
    public static final String MAESTRO_ADAPTER_CLASS_DOC = "";

    public static final String MAESTRO_ADAPTER_FREQUENCY_MS_CONFIG = "maestro.adapter.frequency.ms";
    public static final String MAESTRO_ADAPTER_FREQUENCY_MS_DOC = "";

    public static final String MAESTRO_ADAPTER_HISTORY_SIZE_MAX_CONFIG = "maestro.adapter.history.size.max";
    public static final String MAESTRO_ADAPTER_HISTORY_SIZE_MAX_DOC = "";

    public static final String MAESTRO_ADAPTER_HISTORY_SIZE_MIN_CONFIG = "maestro.adapter.history.size.min";
    public static final String MAESTRO_ADAPTER_HISTORY_SIZE_MIN_DOC = "";

    public static final String MAESTRO_METRICS_COLLECTOR_FREQUENCY_MS_CONFIG = "maestro.metrics.collector.frequency.ms";
    public static final String MAESTRO_METRICS_COLLECTOR_FREQUENCY_MS_DOC = "";

    public static final String NUM_STREAM_THREADS_CONFIG = StreamsConfig.NUM_STREAM_THREADS_CONFIG;
    public static final String NUM_STREAM_THREADS_DOC = "The number of threads to execute stream processing.";

    public static final String ADAPTER_RULE_CLASSES_CONFIG = "maestro.adapter.rule";
    public static final String ADAPTER_RULE_CLASSES_DOC = "A list of classes to use as adapter rules. Implementing the <code>dev.vepo.kafka.maestro.adapter.rules.AdapterRule</code> interface allows plugging in classes that will be used to evaluate adapter rules.";


    public static final Duration DEFAULT_MAESTRO_ADAPTER_TICK_FREQUENCY = Duration.ofMinutes(5);
    public static final Duration DEFAULT_METRICS_COLLECTION_FREQUENCY = Duration.ofSeconds(1);
    public static final int DEFAULT_NUM_STREAM_THREADS = 2;
    public static final int DEFAULT_MAESTRO_ADAPTER_HISTORY_SIZE_MAX = (int) Duration.ofMinutes(60).dividedBy(DEFAULT_METRICS_COLLECTION_FREQUENCY);
    public static final int DEFAULT_MAESTRO_ADAPTER_HISTORY_SIZE_MIN = (int) Duration.ofMinutes(10).dividedBy(DEFAULT_METRICS_COLLECTION_FREQUENCY);

    private static final ConfigDef CONFIG = new ConfigDef().define(CLIENT_ID_CONFIG, Type.STRING, "", Importance.MEDIUM, CommonClientConfigs.CLIENT_ID_DOC)
                                                           .define(ADAPTER_RULE_CLASSES_CONFIG,
                                                                   Type.LIST,
                                                                   null,
                                                                   Importance.LOW,
                                                                   ADAPTER_RULE_CLASSES_DOC)
                                                           .define(MAESTRO_ADAPTER_CLASS_CONFIG,
                                                                   Type.CLASS,
                                                                   Adapter.class,
                                                                   Importance.LOW,
                                                                   MAESTRO_ADAPTER_CLASS_CONFIG)
                                                           .define(MAESTRO_ADAPTER_FREQUENCY_MS_CONFIG,
                                                                   Type.LONG,
                                                                   DEFAULT_MAESTRO_ADAPTER_TICK_FREQUENCY.toMillis(),
                                                                   Importance.MEDIUM,
                                                                   MAESTRO_ADAPTER_FREQUENCY_MS_DOC)
                                                           .define(MAESTRO_METRICS_COLLECTOR_FREQUENCY_MS_CONFIG,
                                                                   Type.LONG,
                                                                   DEFAULT_METRICS_COLLECTION_FREQUENCY.toMillis(),
                                                                   Importance.MEDIUM,
                                                                   MAESTRO_METRICS_COLLECTOR_FREQUENCY_MS_DOC)
                                                           .define(MAESTRO_ADAPTER_HISTORY_SIZE_MAX_CONFIG,
                                                                   Type.INT,
                                                                   DEFAULT_MAESTRO_ADAPTER_HISTORY_SIZE_MAX,
                                                                   Importance.MEDIUM,
                                                                   MAESTRO_ADAPTER_HISTORY_SIZE_MAX_DOC)
                                                           .define(MAESTRO_ADAPTER_HISTORY_SIZE_MIN_CONFIG,
                                                                   Type.INT,
                                                                   DEFAULT_MAESTRO_ADAPTER_HISTORY_SIZE_MIN,
                                                                   Importance.MEDIUM,
                                                                   MAESTRO_ADAPTER_HISTORY_SIZE_MIN_DOC)
                                                           .define(NUM_STREAM_THREADS_CONFIG,
                                                                   Type.INT,
                                                                   DEFAULT_NUM_STREAM_THREADS,
                                                                   Importance.HIGH,
                                                                   NUM_STREAM_THREADS_DOC);

    public MaestroConfigs(Map<?, ?> originals) {
        super(CONFIG, originals);
    }

    public MaestroConfigs(Properties originals) {
        super(CONFIG, originals);
    }

    protected MaestroConfigs(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtml(4, config -> "maestroconfigs_" + config));
    }
}
