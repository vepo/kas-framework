package io.vepo.kafka.maestro.metrics;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vepo.kafka.maestro.MaestroConfigs;

public class PerformanceOptimizer {
    private static final Logger logger = LoggerFactory.getLogger(PerformanceOptimizer.class);
    private int totalThreads = -1;
    private int numberOfGroups = 2;
    private Map<Integer, Map<String, Object>> consumerConfigs;
    private Map<Integer, Map<String, List<PerformanceMetric>>> performanceHistory;
    private Optional<String> parameterName;
    private Optional<Integer> parameterValue;

    public PerformanceOptimizer(Properties props) {
        consumerConfigs = Collections.synchronizedMap(new HashMap<>());
        performanceHistory = Collections.synchronizedMap(new HashMap<>());
        if (props.containsKey(MaestroConfigs.MAESTRO_PARAMETER_NAME_CONFIG) &&
                props.containsKey(MaestroConfigs.MAESTRO_PARAMETER_VALUE_CONFIG)) {
            this.parameterName = Optional.of((String) props.get(MaestroConfigs.MAESTRO_PARAMETER_NAME_CONFIG));
            this.parameterValue = Optional.of((Integer) props.get(MaestroConfigs.MAESTRO_PARAMETER_VALUE_CONFIG));
        } else {
            this.parameterName = Optional.empty();
            this.parameterValue = Optional.empty();
        }
    }

    private Set<String> importantConsumerMetrics = Set.of("records-consumed-rate",
                                                          "bytes-consumed-rate",
                                                          "request-latency-avg",
                                                          "request-latency-max",
                                                          "records-lag-avg",
                                                          "records-lag-max");

    public static final AtomicBoolean collecting = new AtomicBoolean(false);

    /**
     * Need to determine if the metric is growing or decreasing. An rate algorithm
     * neeed to be aware of outliers points.
     * 
     * 
     * https://onlinelibrary.wiley.com/doi/10.1111/j.1744-7976.1985.tb02050.x
     * 
     * @param performanceMetric
     */
    public void feed(PerformanceMetric performanceMetric) {
        if (!collecting.get()) {
            //logger.info("Not colleting metric yet...");
            return;
        }

        // records-consumed-rate
        // bytes-consumed-rate
        if (importantConsumerMetrics.contains(performanceMetric.name())) {
            var groupId = groupId(new StreamClientId(performanceMetric.tags().get("client-id")).threadId());
            var metricHistory = performanceHistory.computeIfAbsent(groupId, (key) -> new HashMap<>())
                                                  .computeIfAbsent(performanceMetric.name(), (key) -> new LinkedList<>());
            if (performanceMetric.value() instanceof Double value && value.isNaN()) {
                return;
            }
            try {
                Files.writeString(Paths.get(".", "data", String.format("metric-%s-%03d-%02d.txt", performanceMetric.name(), currentExecution.get(), groupId)),
                                  String.format("%d %s\n", System.currentTimeMillis(), performanceMetric.value().toString()),
                                  APPEND, CREATE);
            } catch (IOException e) {
                logger.error("Error writing execution info", e);
                System.exit(-1);
            }
            metricHistory.add(performanceMetric);
            while (metricHistory.size() > 100) {
                metricHistory.removeFirst();
            }

            // calculate average value
            if (metricHistory.size() > 0) {
                if (metricHistory.getFirst().value() instanceof Double) {
                    metricHistory.stream()
                                 .mapToDouble(m -> (Double) m.value())
                                 .average()
                                 .ifPresent(average -> {
                                     logger.info("Group {} average {} = (double) {}", groupId, performanceMetric.name(), average);
                                 });
                } else if (metricHistory.getFirst().value() instanceof Long) {
                    metricHistory.stream()
                                 .mapToLong(m -> (Long) m.value())
                                 .average()
                                 .ifPresent(average -> {
                                     logger.info("Group {} average {} = (long) {}", groupId, performanceMetric.name(), average);
                                 });
                } else if (metricHistory.getFirst().value() instanceof Integer) {
                    metricHistory.stream()
                                 .mapToInt(m -> (Integer) m.value())
                                 .average()
                                 .ifPresent(average -> {
                                     logger.info("Group {} average {} = (integer) {}", groupId, performanceMetric.name(), average);
                                 });
                }
            }
        }
    }

    private AtomicInteger currentExecution = new AtomicInteger(0);
    private Set<Integer> infoReportedGroup = new HashSet<>();

    public synchronized Map<String, Object> generateConsumerConfigs(int threadId, Map<String, Object> config) {
        if (currentExecution.get() == 0) {
            int counter = 0;
            while (Paths.get(".", "data", String.format("execution-info-%03d.txt", ++counter)).toFile().exists()) {
                // do nothing
            }
            currentExecution.set(counter);
            try {
                Files.writeString(Paths.get(".", "data", String.format("execution-info-%03d.txt", currentExecution.get())),
                                  """
                                  ==========================================================
                                  Execution Info
                                  ==========================================================

                                  """,
                                  APPEND, CREATE);
            } catch (IOException e) {
                logger.error("Error writing execution info", e);
                System.exit(-1);
            }
        }
        var newConfigs = new HashMap<>(config);
        newConfigs.putAll(consumerConfigs.computeIfAbsent(groupId(threadId), (key) -> newConsumerConfig(key, config)));
        if (infoReportedGroup.add(groupId(threadId))) {
            try {
                Files.writeString(Paths.get(".", "data", String.format("execution-info-%03d.txt", currentExecution.get())),
                                  String.format("""
                                                Group: %02d
                                                Configs: %s

                                                """, groupId(threadId), consumerConfigs.get(groupId(threadId))),
                                  APPEND, CREATE);

            } catch (IOException e) {
                logger.error("Error writing execution info", e);
                System.exit(-1);
            }
        }
        return newConfigs;
    }

    private int groupId(int threadId) {
        return threadId % numberOfGroups;
    }

    // private Set<String> importantConsumerConfigs =
    // Set.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
    // ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
    // ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
    // ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
    // ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
    // ConsumerConfig.RECEIVE_BUFFER_CONFIG,
    // ConsumerConfig.SEND_BUFFER_CONFIG);

    private Set<String> importantConsumerConfigs = Set.of(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);

    private Map<String, Object> newConsumerConfig(int groupId, Map<String, Object> config) {
        // max.poll.interval.ms
        // request.timeout.ms
        // max.poll.records
        if (groupId == 0) {
            return new HashMap<>();
        } else {
            var changeConfigs = new HashMap<String, Object>();
            if (parameterName.isPresent() && parameterValue.isPresent()) {
                changeConfigs.put(parameterName.get(), parameterValue.get());
            } else {
                // Random value between 90% and 110%
                Function<String, Object> randomValue = key -> {
                    var previousValue = config.getOrDefault(key, ConsumerConfig.configDef().defaultValues().get(key));
                    if (previousValue instanceof String) {
                        return (int) ((1.1 - (0.2 * random())) * Integer.parseInt((String) previousValue));
                    } else if (previousValue instanceof Integer) {
                        return (int) ((1.1 - (0.2 * random())) * (Integer) previousValue);
                    } else {
                        logger.warn("Invalid value for key {}: {}", key, previousValue);
                        return previousValue;
                    }
                };
                importantConsumerConfigs.forEach(key -> changeConfigs.put(key, randomValue.apply(key)));
            }
            logger.info("Thread {} changed configs: {}", groupId, changeConfigs);
            return changeConfigs;
        }
    }

    private double random() {
        // 1.1 - 0.2*random()
        return -0.5;
    }

}
