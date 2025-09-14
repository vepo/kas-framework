package dev.vepo.kafka.maestro.metrics;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsRecorder implements MetricListener {
    private static final Logger logger = LoggerFactory.getLogger(MetricsRecorder.class);
    private static final Path DATA_PATH = Paths.get(System.getenv("STATS_FOLDER"));

    @Override
    public void feed(PerformanceMetric metric) {
        try {
            Files.writeString(DATA_PATH.resolve(String.format("metric-%s.txt", metric.id())),
                              String.format("%d %s\n", System.currentTimeMillis(), metric.value().toString()),
                              APPEND, CREATE);
        } catch (IOException e) {
            logger.error("Error writing execution info", e);
            System.exit(-1);
        }
    }

    public enum Event {CHECK, APPLY, RESTART};

    public static void recordEvent(Event event) {
        recordEvent(event, null);
    }

    public static void recordEvent(Event event, Map<?, ?> properties) {
        // write the event to a log file
        try {
            if (Objects.nonNull(properties)){
                Files.writeString(DATA_PATH.resolve("events.txt"),
                                String.format("%d %s %s\n", System.currentTimeMillis(), event.name(), properties.toString()),
                                APPEND, CREATE);
            } else {
                Files.writeString(DATA_PATH.resolve("events.txt"),
                                String.format("%d %s\n", System.currentTimeMillis(), event.name()),
                                APPEND, CREATE);
            }
        } catch (IOException e) {
            logger.error("Error writing execution info", e);
            System.exit(-1);
        }
    }

    @Override
    public void close() {
    }

}
