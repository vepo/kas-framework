package dev.vepo.kafka.maestro.metrics;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.management.OperatingSystemMXBean;

public class EnvironmentMetrics {
    private static final Logger logger = LoggerFactory.getLogger(EnvironmentMetrics.class);
    private final static OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    private final static MemoryMXBean memoryBeans = (MemoryMXBean) ManagementFactory.getMemoryMXBean();
    private final static Object _LOCK = new Object();
    private final static Duration cacheDuration = Duration.ofSeconds(20);
    private final static long PID = ProcessHandle.current().pid();
    private static long lastCpuUsed = 0;
    private static long lastMemoryUsed = 0;
    private static double cpuUsedCached = 0.0;
    private final static long pageSize = loadPageSize();
    private static long totalMemory = loadTotalMemory();
    private static long usedMemory = 0L;

    private static String cgroup() {
        try {
            Path cgroupPath = Paths.get("/proc", Long.toString(PID), "cgroup");
            var cgroupContent = new String(Files.readAllBytes(cgroupPath)).trim();
            logger.info("file {} content {}", cgroupPath, cgroupContent);
            if (!cgroupContent.startsWith("0::/")) {
                throw new IllegalStateException("Not a valid cgroup v2 file: " + cgroupContent);    
            }
            return cgroupContent.substring("0::/".length());
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read cgroup file", e);
        }
    }
    private static long loadTotalMemory() {
        try {
            // it can be 'max', read from another place
            return Long.parseLong(new String(Files.readAllBytes(Paths.get("/sys/fs/cgroup", cgroup(), "memory.max"))).trim());
        } catch (NumberFormatException | IOException e) {
            throw new IllegalStateException("Cannot read cgroup memory max file", e);
        }
    }

    private static long loadPageSize() {
        try {
            var p = new ProcessBuilder("getconf", "PAGESIZE").start();
            if (p.waitFor() == 0) {
                return Long.parseLong(new String(p.getInputStream().readAllBytes()).trim());
            }
            throw new IllegalStateException("Cannot load PAGESIZE");
        } catch (IOException | NumberFormatException | InterruptedException e) {
            throw new IllegalStateException("Cannot load PAGESIZE", e);
        }
    }

    public EnvironmentMetrics() {
    }

    public double cpuUsed() {
        // If no cache is used, a lot of 0 values are returned
        synchronized (_LOCK) {
            if (System.nanoTime() - lastCpuUsed > cacheDuration.toNanos()) {
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
        // return memoryBean.getHeapMemoryUsage().getMax() + memoryBean.getNonHeapMemoryUsage().getMax();
        return totalMemory;
    }

    public long memoryUsed() {
        // return memoryBean.getHeapMemoryUsage().getUsed() + memoryBean.getNonHeapMemoryUsage().getUsed();
        updateMemoryValues();
        return usedMemory;
    }

    private void updateMemoryValues() {
        synchronized (_LOCK) {
            if (System.nanoTime() - lastMemoryUsed > cacheDuration.toNanos()) {
                try {
                    var statmContent = new String(Files.readAllBytes(Paths.get("/proc", Long.toString(PID), "statm"))).split("\s+");
                    if (statmContent.length < 2) {
                        throw new IllegalStateException("Invalid content of statm: " + Arrays.toString(statmContent));
                    }
                    usedMemory = Long.parseLong(statmContent[1]) * pageSize;
                    lastMemoryUsed = System.nanoTime();
                } catch (IOException ioe) {
                    throw new IllegalStateException("Cannot load /proc/" + PID +"/statm", ioe);
                }
            }
        }
    }
}
