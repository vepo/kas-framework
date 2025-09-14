package dev.vepo.kafka.maestro.adapter.rules;

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
            if (avgWaitingThreads > 0.1) {
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
                        var lingerMs = switch (context.instance()
                                                      .originalConfigs()
                                                      .get(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG))) {
                                           case Integer iValue -> iValue;
                                           case Number dValue ->  dValue.intValue();
                                           case String sValue ->  Integer.valueOf(sValue);
                                           default -> 0;
                                       };
                        
                        changes.useValue(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), lingerMs + deltaLinger);
                    }
                }
            }
        }
        return context;
    }

}
