package io.vepo.kafka.stream.loadbalancing;

import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaClientSp implements KafkaClientSupplier {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClientSp.class);

    @Override
    public Admin getAdmin(Map<String, Object> config) {
        LOGGER.info("CLIENTS -> Creating a new admin with config: {} thread={}", config, Thread.currentThread().getName());
        return Admin.create(config);
    }

    @Override
    public Producer<byte[], byte[]> getProducer(Map<String, Object> config) {
        LOGGER.info("CLIENTS -> Creating a new producer with config: {} thread={}", config, Thread.currentThread().getName());
        return new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Override
    public Consumer<byte[], byte[]> getConsumer(Map<String, Object> config) {
        LOGGER.info("CLIENTS -> Creating a new consumer with config: {} thread={}", config, Thread.currentThread().getName());
        return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> config) {
        LOGGER.info("CLIENTS -> Creating a new restore consumer with config: {} thread={}", config, Thread.currentThread().getName());
        return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Override
    public Consumer<byte[], byte[]> getGlobalConsumer(Map<String, Object> config) {
        LOGGER.info("CLIENTS -> Creating a new global consumer with config: {} thread={}", config, Thread.currentThread().getName());
        return new KafkaConsumer<>(config);
    }

}
