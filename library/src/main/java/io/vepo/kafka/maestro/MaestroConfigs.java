package io.vepo.kafka.maestro;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.streams.StreamsConfig;

public class MaestroConfigs extends AbstractConfig {
    public static final String MAESTRO_PERFORMANCE_OPTIMIZER_CONFIG = "maestro.performance.optimizer";
    public static final String MAESTRO_PERFORMANCE_OPTIMIZER_DOC = "";
    public static final int DEFAULT_MAESTRO_TUNING_FREQUENCY_MS = 1000 * 60 * 10;
    public static final String MAESTRO_TUNING_FREQUENCY_MS_CONFIG = "maestro.tuning.frequency.ms";
    public static final String MAESTRO_TUNING_FREQUENCY_MS_DOC = "";
    public static final String NUM_STREAM_THREADS_CONFIG = StreamsConfig.NUM_STREAM_THREADS_CONFIG;
    public static final String NUM_STREAM_THREADS_DOC = "The number of threads to execute stream processing.";
    public static final int DEFAULT_NUM_STREAM_THREADS = 2;

    private static final ConfigDef CONFIG = StreamsConfig.configDef()
                                                         .define(MAESTRO_PERFORMANCE_OPTIMIZER_CONFIG, Type.CLASS, Importance.LOW, MAESTRO_PERFORMANCE_OPTIMIZER_DOC)
                                                         .define(MAESTRO_TUNING_FREQUENCY_MS_CONFIG, Type.INT, DEFAULT_MAESTRO_TUNING_FREQUENCY_MS, Importance.MEDIUM, MAESTRO_TUNING_FREQUENCY_MS_DOC);
    static {
        CONFIG.configKeys().remove(NUM_STREAM_THREADS_CONFIG);
        CONFIG.define(NUM_STREAM_THREADS_CONFIG, Type.INT, DEFAULT_NUM_STREAM_THREADS, Importance.HIGH, NUM_STREAM_THREADS_DOC);
    }

    public MaestroConfigs(Properties props) {
        super(CONFIG, props);
    }

    public MaestroConfigs(Map<String, Object> props) {
        super(CONFIG, props);
    }

    protected MaestroConfigs(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtml(4, config -> "maestroconfigs_" + config));
    }

}
