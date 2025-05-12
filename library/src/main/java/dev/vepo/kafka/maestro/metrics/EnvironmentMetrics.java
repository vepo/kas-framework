package dev.vepo.kafka.maestro.metrics;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import com.sun.management.OperatingSystemMXBean;
import java.time.Duration;

public class EnvironmentMetrics {

    private final static MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private final static OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    private final static Object _LOCK = new Object();
    private final static Duration cpuUsedCache = Duration.ofSeconds(10);
    private static long lastCpuUsed = 0;
    private static double cpuUsedCached = 0.0;

    public EnvironmentMetrics() {
    }

    public double cpuUsed() {
        // If no cache is used, a lot of 0 values are returned
        synchronized (_LOCK) {
            if (System.nanoTime() - lastCpuUsed > cpuUsedCache.toNanos()) {
                cpuUsedCached = osBean.getProcessCpuLoad() * osBean.getAvailableProcessors();
                lastCpuUsed = System.nanoTime();
            }
            return cpuUsedCached;
        }
    }

    public int cpuTotal() {
        return osBean.getAvailableProcessors();
    }

    public long memoryTotal() {
        return memoryBean.getHeapMemoryUsage().getMax();
    }

    public long memoryUsed() {
        return memoryBean.getHeapMemoryUsage().getUsed();
    }
}
