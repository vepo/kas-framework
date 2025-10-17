package dev.vepo.kafka.maestro.metrics;

import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;

public class PerformanceMetricsCollector extends AbstractMaestroMetrics {

    private MetricsRecorder recorder;

    public PerformanceMetricsCollector() {
        this.recorder = null;
    }

    @Override
    protected void process(PerformanceMetric metric) {
        recorder.feed(metric);
    }
    
    @Override
    public void configure(Map<String, ?> configs) {
        var statsFolder = configs.get("metrics.stats.folder");
        Objects.requireNonNull(statsFolder, "statsFolder is required!");
        this.recorder = new MetricsRecorder(Paths.get(statsFolder.toString()));
        super.configure(configs);
    }

    @Override
    public void close() {
        if (Objects.nonNull(recorder)) {
            recorder.close();
        }
        super.close();
    }

}
