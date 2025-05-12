package dev.vepo.kafka.maestro.adapter.rules;

import org.slf4j.Logger;

import dev.vepo.kafka.maestro.adapter.context.StreamsContext;
import dev.vepo.kafka.maestro.adapter.context.ThroughputState;
import dev.vepo.kafka.maestro.adapter.stats.StatsValues;

public class ThroughputAnalyzerRule implements AdapterRule {
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(ThroughputAnalyzerRule.class);

    @Override
    public StreamsContext evaluate(StreamsContext context, RequiredChanges changes) {
        var lagHistory = context.lagHistory().toList();
        if (lagHistory.stream().allMatch(StatsValues::hasData)) {
            if (lagHistory.stream().map(StatsValues::regression).anyMatch(regression -> regression.slope() > 0)) {
                logger.info("Throughput is unsustainable, slope={}", lagHistory.stream().map(StatsValues::regression).mapToDouble(StatsValues.Regression::slope).max());
                return context.withThroughput(ThroughputState.UNSUSTAINABLE);
            } else {
                logger.info("Throughput is sustainable, slope={}", lagHistory.stream().map(StatsValues::regression).mapToDouble(StatsValues.Regression::slope).max());
                return context.withThroughput(ThroughputState.SUSTAINABLE);
            }
        }
        return context;
    }

}
