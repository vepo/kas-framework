package dev.vepo.kafka.maestro.adapter.rules;

import java.util.Objects;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.vepo.kafka.maestro.adapter.context.StreamsContext;
import dev.vepo.kafka.maestro.adapter.context.ThroughputState;

public class BatchProducerRule implements AdapterRule {
    private static final Logger logger = LoggerFactory.getLogger(BatchProducerRule.class);

    @Override
    public StreamsContext evaluate(StreamsContext context, RequiredChanges changes) {
        if (context.throughput() == ThroughputState.UNSUSTAINABLE) {
            var avgWaitingThreads = context.averageWaitingThreads();
            logger.info("Average waiting threads: {}", avgWaitingThreads);
            if (avgWaitingThreads > 0.04) {
                logger.info("Producer is blocking application thread: {}%", avgWaitingThreads);
                var avgBufferAvailable = context.averageBufferAvailable();
                var avgRecordSendRate = context.averageRecordSendRate();
                var avgRecordSize = context.averageRecordSize();
                var avgSendRate = avgRecordSendRate * avgRecordSize;
                if (avgSendRate > 1.0) { // filter irrelevant condition
                    var deltaLinger = 1000 * (avgBufferAvailable / avgSendRate);
                    if (deltaLinger >= 5) { // limit the linger increase to 5
                        logger.info("Limiting delta linger from {} to 5", deltaLinger);
                        deltaLinger = 5;
                    }

                    if (deltaLinger > 1.0) { // linger.ms is an integer
                        var defaultLingerMs = context.instance()
                                                     .originalConfigs()
                                                     .get(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG));
                        logger.info("Previous linger.ms={}", defaultLingerMs);
                        int newLingerValue;
                        if (Objects.nonNull(defaultLingerMs)) {
                            var lingerMs = switch (defaultLingerMs) {
                                               case Integer iValue -> iValue;
                                               case Number dValue -> dValue.intValue();
                                               case String sValue -> Integer.valueOf(sValue);
                                               default -> 0;
                                           };
                            newLingerValue = (int) (lingerMs + deltaLinger);
                        } else {
                            newLingerValue = (int) deltaLinger;
                        }
                        changes.useValue(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), newLingerValue);
                        var requestTimeout = context.instance()
                                                    .originalConfigs()
                                                    .get(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG);
                        int newDeliveryTimeout;
                        if (Objects.nonNull(requestTimeout) && requestTimeout instanceof Number requestTimeoutValue) {
                            newDeliveryTimeout = requestTimeoutValue.intValue() + (2 * newLingerValue);
                        } else {
                            newDeliveryTimeout = 120000 * (2 * newLingerValue);
                        }
                        changes.useValue(StreamsConfig.producerPrefix(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG), newDeliveryTimeout);
                    }
                }
            }
        }
        return context;
    }

}
