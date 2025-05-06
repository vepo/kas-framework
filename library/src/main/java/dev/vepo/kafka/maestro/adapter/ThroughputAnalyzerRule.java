package dev.vepo.kafka.maestro.adapter;

public class ThroughputAnalyzerRule implements AdapterRule {

    @Override
    public StreamsContext evaluate(StreamsContext context) {
        var lagHistory = context.lagHistory().toList();
        if (lagHistory.stream().allMatch(MetricValues::hasData)) {
            if (lagHistory.stream().map(MetricValues::regression).anyMatch(regression -> regression.slope() > 0)) {
                return context.withThroughput(ThroughputState.UNSUSTAINABLE);
            } else {
                return context.withThroughput(ThroughputState.SUSTAINABLE);
            }
        }
        return context;
    }

}
