package dev.vepo.kafka.maestro.metrics;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import dev.vepo.kafka.maestro.MaestroConfigs;
import dev.vepo.kafka.maestro.adapter.Adapter;
import dev.vepo.kafka.maestro.metrics.PerformanceMetric.Context;

@ExtendWith(MockitoExtension.class)
class StreamsHealthyMetricsTest {

    @Mock
    private Adapter mockAdapter;

    @Test
    @DisplayName("The main reporter need to collect JVM metrics")
    void mainReporterTest() {
        try (var metrics = new StreamsHealthyMetrics()) {
            Map<String, Object> configs = new HashMap<>();
            configs.put(MaestroConfigs.MAESTRO_ADAPTER_INSTANCE_CONFIG, mockAdapter);
            configs.put(MaestroConfigs.MAESTRO_METRICS_COLLECTOR_FREQUENCY_MS_CONFIG, 100L);
            configs.put("application.id", "test-app");
            configs.put("client.id", "test-app" + UUID.randomUUID().toString());

            metrics.configure(configs);

            await().pollInterval(50, TimeUnit.MILLISECONDS)
                    .atMost(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> {
                        verify(mockAdapter, times(1))
                                .feed(argThat(m -> m.context() == Context.JVM && m.name().equals("cpu-used")));
                        verify(mockAdapter, times(1))
                                .feed(argThat(m -> m.context() == Context.JVM && m.name().equals("cpu-total")));
                        verify(mockAdapter, times(1))
                                .feed(argThat(m -> m.context() == Context.JVM && m.name().equals("memory-used")));
                        verify(mockAdapter, times(1))
                                .feed(argThat(m -> m.context() == Context.JVM && m.name().equals("memory-total")));
                    });
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "-admin", "-producer", "-consumer" })
    @DisplayName("The producer reporter need to collect JVM metrics {index} {0}")
    void otherReporterTest(String clientIdSuffix) {
        try (var metrics = new StreamsHealthyMetrics()) {
            Map<String, Object> configs = new HashMap<>();
            configs.put(MaestroConfigs.MAESTRO_ADAPTER_INSTANCE_CONFIG, mockAdapter);
            configs.put(MaestroConfigs.MAESTRO_METRICS_COLLECTOR_FREQUENCY_MS_CONFIG, 100L);
            configs.put("application.id", "test-app");
            configs.put("client.id", "test-app" + UUID.randomUUID().toString() + clientIdSuffix);

            metrics.configure(configs);

            await().pollInterval(50, TimeUnit.MILLISECONDS)
                    .atMost(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> {
                        verify(mockAdapter, never())
                                .feed(argThat(m -> m.context() == Context.JVM && m.name().equals("cpu-used")));
                        verify(mockAdapter, never())
                                .feed(argThat(m -> m.context() == Context.JVM && m.name().equals("cpu-total")));
                        verify(mockAdapter, never())
                                .feed(argThat(m -> m.context() == Context.JVM && m.name().equals("memory-used")));
                        verify(mockAdapter, never())
                                .feed(argThat(m -> m.context() == Context.JVM && m.name().equals("memory-total")));
                    });
        }
    }

    private Value value(int value) {
        var mValue = new Value();
        mValue.record(null, value, Instant.now().toEpochMilli());
        return mValue;
    }

    private MetricName recordLagName(String topic, int partition) {
        return new MetricName("records-lag", "consumer-fetch-manager-metrics", "",
                Map.of("topic", topic, "partition", String.valueOf(partition)));
    }

    @Test
    @DisplayName("All partition metrics need to be aggregated by topic")
    void partitionMetricsTest() {
        try (var mReporter = new StreamsHealthyMetrics()) {
            Map<String, Object> configs = new HashMap<>();
            configs.put(MaestroConfigs.MAESTRO_ADAPTER_INSTANCE_CONFIG, mockAdapter);
            configs.put(MaestroConfigs.MAESTRO_METRICS_COLLECTOR_FREQUENCY_MS_CONFIG, 100L);
            configs.put("application.id", "test-app");
            configs.put("client.id", "test-app" + UUID.randomUUID().toString() + "-consumer");

            mReporter.configure(configs);

            var lock = new Object();
            mReporter.init(List.of(new KafkaMetric(lock, recordLagName("test-topic", 0), value(5), null, Time.SYSTEM),
                    new KafkaMetric(lock, recordLagName("test-topic", 1), value(10), null, Time.SYSTEM),
                    new KafkaMetric(lock, recordLagName("test-topic", 2), value(15), null, Time.SYSTEM)));

            await().pollInterval(50, TimeUnit.MILLISECONDS)
                    .atMost(190, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> verify(mockAdapter, times(3))
                            .feed(argThat(
                                    m -> m.context() == Context.PARTITION && m.name().equals("records-lag")
                                            && m.topic().equals("test-topic"))));
        }
    }
}