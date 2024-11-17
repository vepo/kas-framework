package io.vepo.kafka.stream.sample.metrics;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vepo.kafka.stream.sample.SampleStream.Parameter;

public class MetricWriter {

    private static final Logger logger = LoggerFactory.getLogger(MetricWriter.class);
    private static final Path DATA_PATH = Paths.get(System.getenv("STATS_FOLDER"));

    private Set<String> importantConsumerMetrics = Set.of("records-consumed-rate",
            "bytes-consumed-rate",
            "request-latency-avg",
            "request-latency-max",
            "records-lag-avg",
            "records-lag-max",
            "records-lead-min",
            "record-send-rate",
            "record-e2e-latency-avg",
            "emit-final-records-rate",
            "outgoing-byte-rate",
            "incoming-byte-rate",
            "cpu-load",
            "memory-free",
            "memory-usage");

    public void write(PerformanceMetric performanceMetric) {
        writeExecutionInfo();
        if (importantConsumerMetrics.contains(performanceMetric.name())) {
            var groupId = performanceMetric.scope().equals("jvm") || performanceMetric.scope().equals("system") ? 0
                    : new StreamClientId(performanceMetric.clientId()).threadId();
            if (performanceMetric.value() instanceof Double value && value.isNaN()) {
                return;
            }
            try {
                Files.writeString(
                        DATA_PATH.resolve(String.format("metric-%s-%02d.txt", performanceMetric.name(), groupId)),
                        String.format("%d %s\n", System.currentTimeMillis(), performanceMetric.value().toString()),
                        APPEND, CREATE);
            } catch (IOException e) {
                logger.error("Error writing execution info", e);
                System.exit(-1);
            }

        }
    }

    private static final AtomicBoolean executionInfoWrote = new AtomicBoolean(false);

    private void writeExecutionInfo() {
        if (!executionInfoWrote.getAndSet(true)) {
            try {
                Path execInfoFile = DATA_PATH.resolve(String.format("execution-info.txt"));
                Files.writeString(execInfoFile, """
                        ==========================================================
                        Execution Info
                        ==========================================================

                        """,
                        APPEND, CREATE);
                Files.writeString(execInfoFile,
                        String.format("""
                                Configs: %s

                                """,
                                new Parameter(System.getenv("PARAMETER_NAME"),
                                        Integer.parseInt(System.getenv("PARAMETER_VALUE")))),
                        APPEND, CREATE);

            } catch (IOException e) {
                logger.error("Error writing execution info", e);
                System.exit(-1);
            }
        }
    }
}
