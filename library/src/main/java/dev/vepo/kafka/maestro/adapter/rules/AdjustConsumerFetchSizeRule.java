package dev.vepo.kafka.maestro.adapter.rules;

import static org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_FETCH_MAX_BYTES;
import static org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG;

import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.vepo.kafka.maestro.adapter.context.StreamsContext;
import dev.vepo.kafka.maestro.adapter.context.ThroughputState;

public class AdjustConsumerFetchSizeRule implements AdapterRule {

    private static final Logger logger = LoggerFactory.getLogger(AdjustConsumerFetchSizeRule.class);

    @Override
    public StreamsContext evaluate(StreamsContext context, RequiredChanges changes) {
        if (context.throughput() == ThroughputState.UNSUSTAINABLE) {
            var assignedPartitions = context.assignedPartitions();
            var brokersCounter = context.numBrokers();
            logger.info("Adjusting max.partition.fetch.bytes for {} partitions and {} brokers", assignedPartitions, brokersCounter);
    
            var fetchMaxBytes = switch (context.instance()
                                               .originalConfigs()
                                               .getOrDefault(StreamsConfig.consumerPrefix(FETCH_MAX_BYTES_CONFIG),
                                                             DEFAULT_FETCH_MAX_BYTES)) {
                                    case Integer i -> i;
                                    case Number n -> n.intValue();
                                    case String s -> Integer.parseInt(s);
                                    default -> DEFAULT_FETCH_MAX_BYTES;
                                };
    
            var maxPartitionFetchBytes = switch (context.instance()
                                                        .originalConfigs()
                                                        .getOrDefault(StreamsConfig.consumerPrefix(MAX_PARTITION_FETCH_BYTES_CONFIG),
                                                                      DEFAULT_MAX_PARTITION_FETCH_BYTES)) {
                                             case Integer i -> i;
                                             case Number n -> n.intValue();
                                             case String s -> Integer.parseInt(s);
                                             default -> DEFAULT_MAX_PARTITION_FETCH_BYTES;
                                         };
            logger.info("Current max.partition.fetch.bytes={} and fetch.max.bytes={}", maxPartitionFetchBytes, fetchMaxBytes);
            // max.partition.fetch.bytes should not be greater than fetch.max.bytes
            var recommendedMaxPartitionFetchBytes = Math.min((fetchMaxBytes * brokersCounter) / assignedPartitions, fetchMaxBytes);
            if (recommendedMaxPartitionFetchBytes != maxPartitionFetchBytes) {
                logger.info("Adjusting max.partition.fetch.bytes from {} to {}", maxPartitionFetchBytes, recommendedMaxPartitionFetchBytes);
                changes.useValue(StreamsConfig.consumerPrefix(MAX_PARTITION_FETCH_BYTES_CONFIG), recommendedMaxPartitionFetchBytes);
            }
        }
        return context;
    }

}
