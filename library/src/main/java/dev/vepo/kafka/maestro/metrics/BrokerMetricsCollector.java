package dev.vepo.kafka.maestro.metrics;

import static org.apache.kafka.clients.CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

import java.io.Closeable;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.vepo.kafka.maestro.metrics.PerformanceMetric.Context;

public class BrokerMetricsCollector implements Configurable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(BrokerMetricsCollector.class);

    private static AdminClient createAdmin(Map<String, ?> props) {
        var adminProperties = new Properties();
        adminProperties.putAll(props);
        adminProperties.remove(METRIC_REPORTER_CLASSES_CONFIG);
        return AdminClient.create(adminProperties);
    }

    private final MetricListener listener;
    private final AtomicReference<AdminClient> refClient;
    private ScheduledExecutorService executor;
    private boolean topicsDone;
    private boolean brokerDone;

    private Set<String> topics;

    public BrokerMetricsCollector(MetricListener listener, Set<String> topics) {
        this.listener = listener;
        this.topics = topics;
        this.topicsDone = this.brokerDone = false;
        this.refClient = new AtomicReference<>();
        this.executor = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        logger.info("Configuring Brokers Metric Colletor! {}", configs);
        this.topicsDone = this.brokerDone = false;
        this.refClient.set(createAdmin(configs));
        executor.submit(this::verifyTopics);
        executor.submit(this::verifyBroker);
    }

    @Override
    public synchronized void close() {
        var admin = this.refClient.get();
        if (Objects.nonNull(admin)) {
            this.refClient.set(null);
            admin.close();
        }
        if (Objects.nonNull(executor)) {
            this.executor.shutdown();
            try {
                if (!this.executor.awaitTermination(11, TimeUnit.SECONDS)) {
                    this.executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            this.executor = null;
        }
    }

    private void verifyTopics() {
        logger.info("Collecting topic info: topics={}", topics);
        try {
            refClient.get()
                     .describeTopics(topics)
                     .allTopicNames()
                     .get()
                     .forEach((topic, description) -> listener.feed(new PerformanceMetric("broker", "partitions-count", "admin", description.partitions().size(), topic)));
            this.topicsDone = true;
            logger.info("Topic info collected successfully!");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Error requesting topic information! Trying again in 10s...", e);
            executor.schedule(this::verifyTopics, 10, TimeUnit.SECONDS);
        }

        if (topicsDone && brokerDone) {
            this.close();
        }
    }

    private void verifyBroker() {
        logger.info("Collecting broker info!");
        try {
            var nodes = refClient.get()
                    .describeCluster()
                    .nodes()
                    .get();
            listener.feed(new PerformanceMetric(Context.BROKER, "brokers-count", "admin", nodes.size()));
            this.brokerDone = true;
            logger.info("Topic broker collected successfully! nodes={}", nodes);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Error requesting topic information! Trying again in 10s...", e);
            executor.schedule(this::verifyBroker, 10, TimeUnit.SECONDS);
        }

        if (topicsDone && brokerDone) {
            this.close();
        }
    }

}
