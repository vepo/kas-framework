package io.vepo.kafka.maestro;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vepo.kafka.maestro.metrics.PerformanceOptimizer;
import io.vepo.kafka.maestro.metrics.StreamClientId;

public class MaestroClientSupplier implements KafkaClientSupplier {
    private static final Logger logger = LoggerFactory.getLogger(MaestroClientSupplier.class);
    private PerformanceOptimizer optimizer;

    public MaestroClientSupplier(PerformanceOptimizer performanceHistory) {
        this.optimizer = performanceHistory;

        // Consumer
    }

    @Override
    public Admin getAdmin(Map<String, Object> config) {
        return Admin.create(config);
    }

    @Override
    public Producer<byte[], byte[]> getProducer(Map<String, Object> config) {
        logger.info("CLIENTS -> Creating a new producer with config: {} thread={}", config, Thread.currentThread().getName());
        return new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Override
    public Consumer<byte[], byte[]> getConsumer(Map<String, Object> config) {
        logger.info("CLIENTS -> Creating a new consumer with config: {} thread={}", config, Thread.currentThread().getName());
        var clientId = new StreamClientId((String) config.get(ConsumerConfig.CLIENT_ID_CONFIG));
        return new KafkaConsumer<>(optimizer.generateConsumerConfigs(clientId.threadId(), config), new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> config) {
        logger.info("CLIENTS -> Creating a new restore consumer with config: {} thread={}", config, Thread.currentThread().getName());
        return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Override
    public Consumer<byte[], byte[]> getGlobalConsumer(Map<String, Object> config) {
        logger.info("CLIENTS -> Creating a new global consumer with config: {} thread={}", config, Thread.currentThread().getName());
        return new KafkaConsumer<>(config);
    }

}