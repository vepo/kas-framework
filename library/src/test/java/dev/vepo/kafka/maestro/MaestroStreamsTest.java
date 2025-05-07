package dev.vepo.kafka.maestro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import static java.util.Map.entry;

@Testcontainers
public class MaestroStreamsTest {
    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.0.0"));

    private static void withTopic(String topicName, int partitions, short replicationFactor) throws InterruptedException, ExecutionException, TimeoutException {
        try (var admin = KafkaAdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(Collections.singletonList(new NewTopic(topicName, partitions, replicationFactor)))
                 .all().get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void setupTest() throws InterruptedException, ExecutionException, TimeoutException {
        withTopic("input", 10, (short) 1);
        withTopic("output", 10, (short) 1);
        var builder = new StreamsBuilder();
        builder.<String, String>stream("input")
               .flatMapValues(value -> Arrays.asList(value.toLowerCase()
                                                          .replaceAll("[^a-z0-9áéíóúãõâêîôûàèìòùç\\s]", " ")
                                                          .split("\\s+")))
               .groupBy((key, word) -> word)
               .count()
               .toStream()
               .to("output", Produced.with(Serdes.String(), Serdes.Long()));

        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 5);

        try (var streams = new MaestroStreams(builder.build(), props)) {
            streams.cleanUp();
            streams.start();
            withMessage("input", "poem-1", "o rato roeu a roupa do rei de roma");
            withMessage("input", "poem-2", "O rato roeu, roeu, roeu a roupa do rei que ria");
            withMessage("input", "poem-3", "Batatinha quando nasce, espalha rama pelo chão");
            withMessage("input", "poem-4", "Três pratos de trigo para três tigres tristes");

            // Verify results
            Map<String, Long> expectedCounts = Map.ofEntries(entry("o", 2L),
                                                             entry("rato", 2L),
                                                             entry("roeu", 4L),
                                                             entry("a", 2L),
                                                             entry("roupa", 2L),
                                                             entry("do", 2L),
                                                             entry("rei", 2L),
                                                             entry("de", 2L),
                                                             entry("roma", 1L),
                                                             entry("que", 1L),
                                                             entry("ria", 1L),
                                                             entry("batatinha", 1L),
                                                             entry("quando", 1L),
                                                             entry("nasce", 1L),
                                                             entry("espalha", 1L),
                                                             entry("rama", 1L),
                                                             entry("pelo", 1L),
                                                             entry("chão", 1L),
                                                             entry("três", 2L),
                                                             entry("pratos", 1L),
                                                             entry("trigo", 1L),
                                                             entry("para", 1L),
                                                             entry("tigres", 1L),
                                                             entry("tristes", 1L));

            assertWordCounts("output", expectedCounts, Duration.ofSeconds(45));
        }
    }

    private void assertWordCounts(String topic, Map<String, Long> expectedCounts, Duration timeout) {
        // Setup consumer
        var consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "word-count-test-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        try (var consumer = new KafkaConsumer<String, Long>(consumerProps)) {
            consumer.subscribe(Arrays.asList(topic));

            // Wait for all expected words to be processed
            var actualCounts = new HashMap<String, Long>();
            long endTime = System.nanoTime() + timeout.toNanos();

            while (System.nanoTime() < endTime && actualCounts.size() < expectedCounts.size()) {
                consumer.poll(Duration.ofMillis(100))
                        .forEach(record -> actualCounts.put(record.key(), record.value()));
            }

            // Verify counts
            expectedCounts.forEach((word, expectedCount) -> {
                assertTrue(actualCounts.containsKey(word), "Missing word: " + word);
                assertEquals(expectedCount, actualCounts.get(word),
                             "Incorrect count for word: " + word);
            });
        }
    }

    private void withMessage(String topic, String key, String value) throws InterruptedException, ExecutionException {
        try (var producer = new KafkaProducer<String, String>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                                                                     ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                                                                     ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            var metadata = producer.send(new ProducerRecord<String, String>(topic, key, value)).get();
            System.out.println("Message sent! offset:" + metadata.offset() + " partition:" + metadata.partition());
        }
    }
}
