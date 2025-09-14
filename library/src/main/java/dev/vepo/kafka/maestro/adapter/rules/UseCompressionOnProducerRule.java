package dev.vepo.kafka.maestro.adapter.rules;

import java.util.Objects;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

import dev.vepo.kafka.maestro.adapter.context.StreamsContext;
import dev.vepo.kafka.maestro.adapter.context.ThroughputState;

public class UseCompressionOnProducerRule implements AdapterRule {

    @Override
    public StreamsContext evaluate(StreamsContext context, RequiredChanges changes) {
        if (context.throughput() == ThroughputState.UNSUSTAINABLE) {
            var compressionType = context.instance()
                                         .originalConfigs()
                                         .get(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG));
            if (Objects.isNull(compressionType) ||
                    (compressionType instanceof String sCompressionType && sCompressionType.equalsIgnoreCase("none"))) {
                changes.useValue(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "snappy");
            }
        }
        return context;
    }

}
