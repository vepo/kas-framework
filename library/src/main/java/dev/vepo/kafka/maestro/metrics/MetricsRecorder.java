package dev.vepo.kafka.maestro.metrics;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsRecorder implements MetricListener {
    private static final Logger logger = LoggerFactory.getLogger(MetricsRecorder.class);
    private static final Path DATA_PATH = Paths.get(System.getenv("STATS_FOLDER"));

    @Override
    public void feed(PerformanceMetric metric) {
        try {
            Files.writeString(DATA_PATH.resolve(String.format("metric-%s.txt", metric.name())),
                              String.format("%d %s\n", System.currentTimeMillis(), metric.value().toString()),
                              APPEND, CREATE);
        } catch (IOException e) {
            logger.error("Error writing execution info", e);
            System.exit(-1);
        }
    }

    @Override
    public void close() {
    }

}
