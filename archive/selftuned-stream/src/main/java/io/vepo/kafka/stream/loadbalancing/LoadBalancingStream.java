package io.vepo.kafka.stream.loadbalancing;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.state.BuiltInDslStoreSuppliers.InMemoryDslStoreSuppliers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadBalancingStream implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancingStream.class);

    private KafkaStreams streams;
    private CountDownLatch latch;
    private ScheduledExecutorService managementServices;

    public static record Speed(String measurementId,
                               String internalId,
                               long publishTimestamp,
                               double latitude,
                               double longitude,
                               int speed,
                               int accuracy,
                               int numLanes) {
        long roundedTimestamp() {
            return Duration.ofMillis(publishTimestamp).toSeconds();
        }
    }

    public static record Flow(String measurementId,
                              String internalId,
                              long publishTimestamp,
                              double latitude,
                              double longitude,
                              int flow,
                              int period,
                              int accuracy,
                              int numLanes) {
        long roundedTimestamp() {
            return Duration.ofMillis(publishTimestamp).toSeconds();
        }
    }

    public static record Observation(String measurementId,
                                     List<String> lanes,
                                     long publishTimestamp,
                                     double latitude,
                                     double longitude,
                                     int accumulatedFlow,
                                     int period,
                                     int flowAccuracy,
                                     double averageSpeed,
                                     int speedAccuracy,
                                     int numLanes) {

        public Observation(Flow flow, Speed speed) {
            this(flow.measurementId(),
                 List.of(flow.internalId()),
                 Math.max(flow.publishTimestamp(), speed.publishTimestamp()),
                 flow.latitude(),
                 flow.longitude(),
                 flow.flow(),
                 flow.period(),
                 flow.accuracy(),
                 speed.speed(),
                 speed.accuracy(),
                 checkConsistentLanes(flow.numLanes(), speed.numLanes()));
        }

        private static int checkConsistentLanes(int flowLanes, int speedLanes) {
            return flowLanes == speedLanes ? speedLanes : -1;
        }

        public Observation combineObservations(Observation aggregatedObservation) {
            var lanes = new ArrayList<String>(this.lanes);
            lanes.addAll(aggregatedObservation.lanes);
            return new Observation(this.measurementId,
                                   lanes,
                                   Math.max(this.publishTimestamp, aggregatedObservation.publishTimestamp),
                                   this.latitude,
                                   this.longitude,
                                   this.period == aggregatedObservation.period ? this.period : -1,
                                   this.accumulatedFlow + aggregatedObservation.accumulatedFlow,
                                   this.flowAccuracy == aggregatedObservation.flowAccuracy ? this.flowAccuracy : -1,
                                   (this.averageSpeed * this.lanes.size()
                                           + aggregatedObservation.averageSpeed * aggregatedObservation.lanes.size())
                                           / (this.lanes.size() + aggregatedObservation.lanes.size()),
                                   this.speedAccuracy == aggregatedObservation.speedAccuracy ? this.speedAccuracy : -1,
                                   this.numLanes);
        }
    }

    public LoadBalancingStream() {
        streams = null;
        latch = null;
        managementServices = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void close() {
        if (Objects.nonNull(streams)) {
            latch.countDown();
            streams.close();
        }
    }

    Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Flow> flowStream = builder.stream("flow");
        KStream<String, Speed> speedStream = builder.stream("speed");
        flowStream.selectKey((k, v) -> {
            logger.info("Processing Flow: {}", v);
            return v.measurementId() + "/" + v.internalId() + "/" + v.roundedTimestamp();
        })
                  .join(speedStream.selectKey((k, v) -> {
                      logger.info("Processing Speed: {}", v);
                      return v.measurementId() + "/" + v.internalId() + "/" + v.roundedTimestamp();
                  }),
                        Observation::new,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.of(5, ChronoUnit.MINUTES)),
                        StreamJoined.as("observations-repartition"))
                  .groupByKey()
                  .reduce(Observation::combineObservations)
                  .toStream()
                  .to("observation");
        return builder.build();
    }

    public void start(Properties props) {
        streams = new KafkaStreams(buildTopology(), props, new KafkaClientSp());
        latch = new CountDownLatch(1);
        var startTime = new AtomicLong( System.currentTimeMillis());
        

        this.managementServices.scheduleAtFixedRate(() -> {
            ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

            // streams.metadataForLocalThreads().forEach(m -> {
            // System.out.println(m.threadName());
            // // System.out.println(String.format("CPU time: %s ns",
            // // threadMXBean.getThreadCpuTime(m.threadName())));
            // });
            // for(Long threadID : threadMXBean.getAllThreadIds()) {
            // ThreadInfo info = threadMXBean.getThreadInfo(threadID);
            // System.out.println("Thread name: " + info.getThreadName());
            // System.out.println("Thread State: " + info.getThreadState());
            // System.out.println(String.format("CPU time: %s ns",
            // threadMXBean.getThreadCpuTime(threadID)));
            // }
            if (TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - startTime.get()) > 5) {
                streams.addStreamThread();
                startTime.set(System.currentTimeMillis());
            }
        }, 10, 10, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static class LocalMetricsReporter implements MetricsReporter, ClusterResourceListener {

        private Map<String, KafkaMetric> metrics;
        private ScheduledExecutorService executor;
        private static final Object LOCK = new Object();
        private String prefix;

        public LocalMetricsReporter() {
            prefix = "";
            metrics = Collections.synchronizedMap(new HashMap<>());
            executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(() -> {
                metrics.forEach((k, v) -> {
                    if (!k.contains("records-lag") && !k.contains("records-lead")) {
                        return;
                    }
                    synchronized (LocalMetricsReporter.class) {
                        try {
                            Files.write(Paths.get("metrics.txt"),
                                        String.format("%s: %s\n", k, v.metricValue()).getBytes(),
                                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                        } catch (IOException e) {
                            // exception handling left as an exercise for the reader
                            e.printStackTrace();
                        }
                    }
                });
            }, 10, 10, TimeUnit.SECONDS);
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // System.out.println("Configuring metrics reporter: " + configs);
        }

        @Override
        public void init(List<KafkaMetric> metrics) {
            metrics.forEach(this::metricChange);
        }

        @Override
        public void metricChange(KafkaMetric metric) {
            synchronized (LOCK) {
                metrics.put(getMetricName(prefix, metric.metricName()), metric);
            }
        }

        private static String getMetricName(String prefix, MetricName metricName) {
            StringBuilder name = new StringBuilder();
            name.append(prefix);
            name.append(":");
            name.append("name=");
            name.append(metricName.name());
            name.append(":type=");
            name.append(metricName.group());
            for (Map.Entry<String, String> entry : metricName.tags().entrySet()) {
                if (entry.getKey().length() > 0 && entry.getValue().length() > 0) {
                    name.append(",");
                    name.append(entry.getKey());
                    name.append("=");
                    name.append(entry.getValue());
                }
            }
            return name.toString();
        }

        @Override
        public void metricRemoval(KafkaMetric metric) {
            synchronized (LOCK) {
                metrics.remove(metric.metricName().name());
            }
        }

        @Override
        public void close() {
            executor.shutdown();
        }

        @Override
        public void onUpdate(ClusterResource clusterResource) {
            // System.out.println("Cluster resource update: " + clusterResource);
        }

        @Override
        public void contextChange(MetricsContext metricsContext) {
            String namespace = metricsContext.contextLabels().get(MetricsContext.NAMESPACE);
            Objects.requireNonNull(namespace);
            synchronized (LOCK) {
                if (namespace.isEmpty()) {
                    return;
                }

                prefix = namespace;
            }
        }

    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0:9092,kafka-1:9094,kafka-2:9096");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "load-balancing-0");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        props.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, LocalMetricsReporter.class.getName());
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "TRACE");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
            //props.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, InMemoryDslStoreSuppliers.class);
        try (var processor = new LoadBalancingStream()) {
            processor.start(props);
        }
    }
}