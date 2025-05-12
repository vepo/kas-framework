///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS org.jfree:jfreechart:1.5.4
//DEPS org.jfree:jcommon:1.0.24
//DEPS org.apache.commons:commons-math3:3.6.1

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.jfree.data.time.*;
import org.jfree.data.xy.XYDataset;
import java.awt.Color;
import java.awt.BasicStroke;
import java.awt.Paint;
import java.io.*;
import java.nio.file.*;
import java.text.*;
import java.util.*;
import java.util.stream.Collectors;

public class ProcessData {

        // Vanilla color set (pastel blues/greens with alpha)
    private static final Paint[] VANILLA_COLLORS = new Paint[] {
            new Color(173, 216, 230, 200), // LightBlue
            new Color(144, 238, 144, 200), // LightGreen
            new Color(175, 238, 238, 200), // PaleTurquoise
            new Color(152, 251, 152, 200), // PaleGreen
            new Color(176, 224, 230, 200), // PowderBlue
            new Color(127, 255, 212, 200), // Aquamarine
            new Color(193, 255, 193, 200), // MintCream
            new Color(135, 206, 250, 200), // LightSkyBlue
            new Color(147, 197, 114, 200), // SageGreen
            new Color(102, 205, 170, 200), // MediumAquamarine
            new Color(176, 196, 222, 200), // LightSteelBlue
            new Color(143, 188, 143, 200), // DarkSeaGreen
            new Color(100, 149, 237, 200), // CornflowerBlue
            new Color(95, 158, 160, 200),  // CadetBlue
            new Color(72, 209, 204, 200),  // MediumTurquoise
            new Color(64, 224, 208, 200)   // Turquoise
        };

        // Maestro color set (pastel reds/purples with alpha)
        private static final Paint[] MAESTRO_COLLORS = new Paint[] {
            new Color(255, 182, 193, 200), // LightPink
            new Color(255, 160, 122, 200), // LightSalmon
            new Color(255, 192, 203, 200), // Pink
            new Color(250, 128, 114, 200), // Salmon
            new Color(221, 160, 221, 200), // Plum
            new Color(255, 105, 180, 200), // HotPink
            new Color(240, 128, 128, 200), // LightCoral
            new Color(216, 191, 216, 200), // Thistle
            new Color(255, 127, 80, 200),  // Coral
            new Color(219, 112, 147, 200), // PaleVioletRed
            new Color(199, 21, 133, 200),  // MediumVioletRed
            new Color(238, 130, 238, 200), // Violet
            new Color(218, 165, 32, 200),  // GoldenRod
            new Color(205, 92, 92, 200),   // IndianRed
            new Color(255, 99, 71, 200),   // Tomato
            new Color(255, 69, 0, 200)     // OrangeRed
        };

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: jbang EnhancedMetricsComparisonPlotter.java <vanilla-stats-dir> <maestro-stats-dir> [output-dir]");
            System.exit(1);
        }

        String vanillaDir = args[0];
        String maestroDir = args[1];
        String outputDir = args.length > 2 ? args[2] : ".";

        // Create output directory if it doesn't exist
        Files.createDirectories(Paths.get(outputDir));

        // Process and plot metrics
        plotEnhancedLagMetrics(vanillaDir, maestroDir, outputDir);
        plotCpuMetrics(vanillaDir, maestroDir, outputDir);
        plotMemoryMetrics(vanillaDir, maestroDir, outputDir);
        plotConsumedRateMetrics(vanillaDir, maestroDir, outputDir);

        System.out.println("All charts generated in: " + outputDir);
    }

    private static void plotConsumedRateMetrics(String vanillaDir, String maestroDir, String outputDir) throws IOException {
        TimeSeriesCollection dataset = new TimeSeriesCollection();

        // Vanilla consumed rate
        TimeSeries vanillaSeries = calculateTotalConsumedRate(
            vanillaDir + "/metric-records-consumed-rate-vehicle-moviment.txt",
            vanillaDir + "/metric-alive-stream-threads.txt",
            "Vanilla Total Consumed Rate"
        );
        dataset.addSeries(vanillaSeries);

        // Maestro consumed rate
        TimeSeries maestroSeries = calculateTotalConsumedRate(
            maestroDir + "/metric-records-consumed-rate-vehicle-moviment.txt",
            maestroDir + "/metric-alive-stream-threads.txt",
            "Maestro Total Consumed Rate"
        );
        dataset.addSeries(maestroSeries);

        JFreeChart chart = createConsumedRateChart(dataset, "Total Records Consumed Rate Comparison", "Time", "Records/s");
        ChartUtils.saveChartAsPNG(new File(outputDir, "total_consumed_rate_comparison.png"), chart, 1000, 600);
    }

    private static TimeSeries calculateTotalConsumedRate(String consumedRateFile, String threadsFile, String seriesName) throws IOException {
        // Read both metrics
        List<DataPoint> ratePoints = readDataPoints(consumedRateFile);
        List<DataPoint> threadPoints = readDataPoints(threadsFile);

        // Create maps for efficient lookup
        Map<Long, Double> rateMap = ratePoints.stream()
            .collect(Collectors.toMap(
                p -> p.date.getTime(),
                p -> p.value,
                (v1, v2) -> v1 // handle duplicates by taking first value
            ));

        Map<Long, Double> threadMap = threadPoints.stream()
            .collect(Collectors.toMap(
                p -> p.date.getTime(),
                p -> p.value,
                (v1, v2) -> v1
            ));

        // Find all unique timestamps from both files
        Set<Long> allTimestamps = new HashSet<>();
        allTimestamps.addAll(rateMap.keySet());
        allTimestamps.addAll(threadMap.keySet());

        // Calculate total consumed rate (rate * threads)
        TimeSeries resultSeries = new TimeSeries(seriesName);
        allTimestamps.stream()
            .sorted()
            .forEach(timestamp -> {
                Double rate = rateMap.get(timestamp);
                Double threads = threadMap.get(timestamp);
                
                if (rate != null && threads != null) {
                    double totalRate = rate * threads;
                    resultSeries.add(new Millisecond(new Date(timestamp)), totalRate);
                }
            });

        return resultSeries;
    }

    private static JFreeChart createConsumedRateChart(XYDataset dataset, String title, String xAxis, String yAxis) {
        JFreeChart chart = ChartFactory.createTimeSeriesChart(
            title,
            xAxis,
            yAxis,
            dataset,
            true,
            true,
            false
        );

        XYPlot plot = chart.getXYPlot();
        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        
        Paint[] colors = new Paint[] {
            new Color(70, 130, 180, 200),  // Steel Blue for Vanilla
            new Color(220, 20, 60, 200)    // Crimson for Maestro
        };
        
        for (int i = 0; i < dataset.getSeriesCount(); i++) {
            renderer.setSeriesPaint(i, colors[i % colors.length]);
            renderer.setSeriesStroke(i, new BasicStroke(2.5f));
            renderer.setSeriesShapesVisible(i, false);
        }
        
        plot.setRenderer(renderer);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.LIGHT_GRAY);
        plot.setRangeGridlinePaint(Color.LIGHT_GRAY);
        plot.getRangeAxis().setLowerMargin(0.1);
        plot.getRangeAxis().setUpperMargin(0.1);

        return chart;
    }

    private static void plotEnhancedLagMetrics(String vanillaDir, String maestroDir, String outputDir) throws IOException {
        TimeSeriesCollection dataset = new TimeSeriesCollection();

        // Process Vanilla lag metrics
        Map<String, TimeSeries> vanillaSeries = processLagFilesByPartition(vanillaDir, "Vanilla");
        addSeriesToDataset(dataset, vanillaSeries);
        addAverageSeries(dataset, vanillaSeries, "Vanilla Average");

        // Process Maestro lag metrics
        Map<String, TimeSeries> maestroSeries = processLagFilesByPartition(maestroDir, "Maestro");
        addSeriesToDataset(dataset, maestroSeries);
        addAverageSeries(dataset, maestroSeries, "Maestro Average");

        // Create and save chart
        JFreeChart chart = createEnhancedChart(dataset, "Vehicle Movement Lag Comparison (By Partition)", "Time", "Lag (records)");
        ChartUtils.saveChartAsPNG(new File(outputDir, "enhanced_lag_comparison.png"), chart, 1200, 800);
    }

    private static Map<String, TimeSeries> processLagFilesByPartition(String dir, String groupName) throws IOException {
        Map<String, TimeSeries> seriesMap = new TreeMap<>();
        File directory = new File(dir);
        File[] files = directory.listFiles((d, name) -> name.startsWith("metric-records-lag-vehicle-moviment-") && name.endsWith(".txt"));
        
        if (files == null || files.length == 0) {
            System.err.println("No lag files found in " + dir);
            return seriesMap;
        }

        for (File file : files) {
            String partition = file.getName().replace("metric-records-lag-vehicle-moviment-", "").replace(".txt", "");
            TimeSeries series = new TimeSeries(groupName + " Partition " + partition);
            List<DataPoint> points = readDataPoints(file.getAbsolutePath());
            for (DataPoint point : points) {
                series.add(new Millisecond(point.date), point.value);
            }
            seriesMap.put(partition, series);
        }

        return seriesMap;
    }

    private static void addSeriesToDataset(TimeSeriesCollection dataset, Map<String, TimeSeries> seriesMap) {
        int count = 0;
        int total = seriesMap.size();
        for (TimeSeries series : seriesMap.values()) {            
            dataset.addSeries(series);
        }
    }

    private static void addAverageSeries(TimeSeriesCollection dataset, Map<String, TimeSeries> seriesMap, String name) {
        if (seriesMap.isEmpty()) return;

        // Step 1: Collect all data points (timestamp -> value)
        List<DataPoint> allPoints = new ArrayList<>();
        for (TimeSeries series : seriesMap.values()) {
            for (int i = 0; i < series.getItemCount(); i++) {
                Millisecond period = (Millisecond) series.getDataItem(i).getPeriod();
                double value = series.getDataItem(i).getValue().doubleValue();
                allPoints.add(new DataPoint(period.getStart(), value));
            }
        }
    
        if (allPoints.isEmpty()) return;
    
        // Step 2: Prepare data for linear regression
        double[] x = new double[allPoints.size()];
        double[] y = new double[allPoints.size()];
        
        for (int i = 0; i < allPoints.size(); i++) {
            x[i] = allPoints.get(i).date.getTime(); // Time in ms since epoch
            y[i] = allPoints.get(i).value;
        }
    
        // Step 3: Calculate linear regression
        SimpleRegression regression = new SimpleRegression();
        for (int i = 0; i < x.length; i++) {
            regression.addData(x[i], y[i]);
        }
    
        // Step 4: Create regression time series
        TimeSeries regressionSeries = new TimeSeries(name);
        
        // Get min/max timestamps to define regression range
        long minTime = allPoints.stream()
                              .mapToLong(p -> p.date.getTime())
                              .min()
                              .orElse(0);
        long maxTime = allPoints.stream()
                              .mapToLong(p -> p.date.getTime())
                              .max()
                              .orElse(minTime + 1);
    
        // Add points at regular intervals (100 points across the range)
        int numPoints = 100;
        for (int i = 0; i <= numPoints; i++) {
            long time = minTime + (i * (maxTime - minTime) / numPoints);
            double predictedValue = regression.predict(time);
            regressionSeries.add(new Millisecond(new Date(time)), predictedValue);
        }
    
        // Step 5: Add to dataset with styling
        dataset.addSeries(regressionSeries);
    }

    private static void plotCpuMetrics(String vanillaDir, String maestroDir, String outputDir) throws IOException {
        TimeSeriesCollection dataset = new TimeSeriesCollection();

        // Vanilla CPU metrics
        TimeSeries vanillaCpu = readSingleMetricFile(vanillaDir + "/metric-cpu-used.txt", "Vanilla CPU Used");
        TimeSeries vanillaCpuTotal = readSingleMetricFile(vanillaDir + "/metric-cpu-total.txt", "Vanilla CPU Total");
        dataset.addSeries(vanillaCpu);
        dataset.addSeries(vanillaCpuTotal);

        // Maestro CPU metrics
        TimeSeries maestroCpu = readSingleMetricFile(maestroDir + "/metric-cpu-used.txt", "Maestro CPU Used");
        TimeSeries maestroCpuTotal = readSingleMetricFile(maestroDir + "/metric-cpu-total.txt", "Maestro CPU Total");
        dataset.addSeries(maestroCpu);
        dataset.addSeries(maestroCpuTotal);

        // Create and save chart
        JFreeChart chart = createChart(dataset, "CPU Usage Comparison", "Time", "CPU Usage");
        ChartUtils.saveChartAsPNG(new File(outputDir, "cpu_comparison.png"), chart, 1000, 600);
    }

    private static void plotMemoryMetrics(String vanillaDir, String maestroDir, String outputDir) throws IOException {
        TimeSeriesCollection dataset = new TimeSeriesCollection();

        // Vanilla Memory metrics
        TimeSeries vanillaMemUsed = readSingleMetricFile(vanillaDir + "/metric-memory-used.txt", "Vanilla Memory Used");
        TimeSeries vanillaMemTotal = readSingleMetricFile(vanillaDir + "/metric-memory-total.txt", "Vanilla Memory Total");
        dataset.addSeries(vanillaMemUsed);
        dataset.addSeries(vanillaMemTotal);

        // Maestro Memory metrics
        TimeSeries maestroMemUsed = readSingleMetricFile(maestroDir + "/metric-memory-used.txt", "Maestro Memory Used");
        TimeSeries maestroMemTotal = readSingleMetricFile(maestroDir + "/metric-memory-total.txt", "Maestro Memory Total");
        dataset.addSeries(maestroMemUsed);
        dataset.addSeries(maestroMemTotal);

        // Create and save chart
        JFreeChart chart = createChart(dataset, "Memory Usage Comparison", "Time", "Memory Usage (bytes)");
        ChartUtils.saveChartAsPNG(new File(outputDir, "memory_comparison.png"), chart, 1000, 600);
    }

    private static TimeSeries readSingleMetricFile(String filePath, String seriesName) throws IOException {
        TimeSeries series = new TimeSeries(seriesName);
        List<DataPoint> points = readDataPoints(filePath);
        for (DataPoint point : points) {
            series.add(new Millisecond(point.date), point.value);
        }
        return series;
    }

    private static List<DataPoint> readDataPoints(String filename) throws IOException {
        List<DataPoint> points = new ArrayList<>();

        for (String line : Files.readAllLines(Paths.get(filename))) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) continue;

            String[] parts = line.split("\\s+", 2);
            if (parts.length != 2) continue;

            try {
                long timestamp = Long.parseLong(parts[0]);
                double value = Double.parseDouble(parts[1]);
                points.add(new DataPoint(new Date(timestamp), value));
            } catch (NumberFormatException e) {
                System.err.println("Skipping invalid line in " + filename + ": " + line);
            }
        }

        return points;
    }

    private static JFreeChart createChart(XYDataset dataset, String title, String xAxis, String yAxis) {
        JFreeChart chart = ChartFactory.createTimeSeriesChart(
            title,
            xAxis,
            yAxis,
            dataset,
            true,
            true,
            false
        );

        // Customize plot appearance
        XYPlot plot = chart.getXYPlot();
        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        
        // Set different colors for each series
        Paint[] colors = new Paint[] {
            Color.BLUE,    // Vanilla
            Color.RED,      // Maestro
            Color.GREEN,   // Vanilla Total
            Color.ORANGE    // Maestro Total
        };
        
        for (int i = 0; i < dataset.getSeriesCount(); i++) {
            renderer.setSeriesPaint(i, colors[i % colors.length]);
            renderer.setSeriesStroke(i, new BasicStroke(2.0f));
        }
        
        plot.setRenderer(renderer);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.LIGHT_GRAY);
        plot.setRangeGridlinePaint(Color.LIGHT_GRAY);

        return chart;
    }

    private static JFreeChart createEnhancedChart(XYDataset dataset, String title, String xAxis, String yAxis) {
        JFreeChart chart = ChartFactory.createTimeSeriesChart(
            title,
            xAxis,
            yAxis,
            dataset,
            true,
            true,
            false
        );

        XYPlot plot = chart.getXYPlot();
        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        
        // Configure renderer for all series
        for (int i = 0; i < dataset.getSeriesCount(); i++) {
            String seriesName = dataset.getSeriesKey(i).toString();
            
            if (seriesName.contains("Average")) {
                // Make average series stand out
                if (seriesName.contains("Vanilla")) {
                    renderer.setSeriesPaint(i, Color.BLUE);
                } else if (seriesName.contains("Maestro")) {
                    renderer.setSeriesPaint(i, Color.RED);
                } else {
                    renderer.setSeriesPaint(i, Color.GRAY);
                }
                renderer.setSeriesStroke(i, new BasicStroke(3.0f));
                renderer.setSeriesShapesVisible(i, false);
            } else {
                if (seriesName.contains("Vanilla")) {
                    renderer.setSeriesPaint(i, VANILLA_COLLORS[i % VANILLA_COLLORS.length]);
                } else if (seriesName.contains("Maestro")) {
                    renderer.setSeriesPaint(i, MAESTRO_COLLORS[i % MAESTRO_COLLORS.length]);
                } else {
                    renderer.setSeriesPaint(i, Color.GRAY);
                }
                // Partition series - already colored when added
                renderer.setSeriesStroke(i, new BasicStroke(1.5f));
                renderer.setSeriesShapesVisible(i, false);
            }
        }
        
        plot.setRenderer(renderer);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.LIGHT_GRAY);
        plot.setRangeGridlinePaint(Color.LIGHT_GRAY);
        
        // Add some margin to the Y axis
        plot.getRangeAxis().setLowerMargin(0.1);
        plot.getRangeAxis().setUpperMargin(0.1);

        return chart;
    }

    static class DataPoint {
        Date date;
        double value;

        DataPoint(Date date, double value) {
            this.date = date;
            this.value = value;
        }
    }
}