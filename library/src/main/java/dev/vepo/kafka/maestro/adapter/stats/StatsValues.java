package dev.vepo.kafka.maestro.adapter.stats;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatsValues {

    private static final Logger logger = LoggerFactory.getLogger(StatsValues.class);

    private record Point(double x, double y) {
    }

    public class Regression {

        private final SimpleRegression regression;

        public Regression(SimpleRegression regression) {
            this.regression = regression;
        }

        public double slope() {
            return regression.getSlope();
        }

        public double root() {
            return -regression.getIntercept() / regression.getSlope();
        }
    }

    private final LinkedList<Point> values;
    private final int maxSize;
    private final int minSize;

    public StatsValues(int minSize, int maxSize) {
        this.values = new LinkedList<>();
        this.maxSize = maxSize;
        this.minSize = minSize;
    }

    public void add(Number value, long timestamp) {
        if (this.values.size() == maxSize) {
            this.values.removeFirst();
        }
        values.addLast(new Point((double) timestamp, value.doubleValue()));
    }

    public void clear() {
        this.values.clear();
    }

    public boolean hasData() {
        return this.values.size() >= minSize;
    }

    public Double average() {
        return values.stream()
                     .mapToDouble(Point::y)
                     .average()
                     .orElse(0.0);
    }

    public List<Double> all() {
        return values.stream().map(Point::y).toList();
    }

    public Double last() {
        return values.isEmpty() ? 0.0 : values.getLast().y;
    }

    public Regression regression() {
        // logger.info("Calculating regression with {} points", values);
        // Do not cache SimpleRegression
        // For long running the imprecision can grow
        // This is not called all the time
        var regression = new SimpleRegression(true);
        regression.addData(values.stream()
                                 .map(v -> new double[] {
                                     v.x(),
                                     v.y() })
                                 .toArray(double[][]::new));
        // logger.info("Regression: slope={}", regression.getSlope());
        return new Regression(regression);
    }
}
