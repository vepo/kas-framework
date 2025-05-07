package dev.vepo.kafka.maestro.adapter;

import org.slf4j.Logger;

public class ThroughputAnalyzerRule implements AdapterRule {
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(ThroughputAnalyzerRule.class);

    @Override
    public StreamsContext evaluate(StreamsContext context, RequiredChanges changes) {
        var lagHistory = context.lagHistory().toList();
        if (lagHistory.stream().allMatch(MetricValues::hasData)) {
            if (lagHistory.stream().map(MetricValues::regression).anyMatch(regression -> regression.slope() > 0)) {
                logger.info("Throughput is unsustainable, slope={}", lagHistory.stream().map(MetricValues::regression).mapToDouble(MetricValues.Regression::slope).max());
                return context.withThroughput(ThroughputState.UNSUSTAINABLE);
            } else {
                logger.info("Throughput is sustainable, slope={}", lagHistory.stream().map(MetricValues::regression).mapToDouble(MetricValues.Regression::slope).max());
                return context.withThroughput(ThroughputState.SUSTAINABLE);
            }
        }
        return context;
    }

}
