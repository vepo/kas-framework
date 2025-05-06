package dev.vepo.kafka.maestro.adapter;

import java.util.LinkedList;

import org.apache.commons.math3.stat.regression.SimpleRegression;

public class MetricValues {

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

    public MetricValues(int minSize, int maxSize) {
        this.values = new LinkedList<>();
        this.maxSize = maxSize;
        this.minSize = minSize;
    }

    public void add(Number value, long timestamp) {
        if (this.values.size() == maxSize) {
            this.values.removeFirst();
        }
        values.addLast(new Point(value.doubleValue(), (double) timestamp));
    }

    public boolean hasData() {
        return this.values.size() >= minSize;
    }

    public Regression regression() {
        var regression = new SimpleRegression(true);
        regression.addData(values.stream()
                                 .map(v -> new double[] {
                                     v.y(),
                                     v.x() })
                                 .toArray(double[][]::new));
        return new Regression(regression);
    }
}
