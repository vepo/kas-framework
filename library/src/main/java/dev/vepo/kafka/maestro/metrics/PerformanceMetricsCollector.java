package dev.vepo.kafka.maestro.metrics;

public class PerformanceMetricsCollector extends AbstractMaestroMetrics {

    private final MetricsRecorder recorder;

    public PerformanceMetricsCollector() {
        this.recorder = new MetricsRecorder();
    }

    @Override
    protected void process(PerformanceMetric metric) {
        recorder.feed(metric);
    }

    @Override
    public void close() {
        recorder.close();
        super.close();
    }

}
