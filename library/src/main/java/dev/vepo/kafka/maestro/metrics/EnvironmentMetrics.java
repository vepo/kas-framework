package dev.vepo.kafka.maestro.metrics;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;

public class EnvironmentMetrics {

    private final CentralProcessor processor;
    private long[] prevTicks;
    private final GlobalMemory memory;

    public EnvironmentMetrics() {
        SystemInfo systemInfo = new SystemInfo();
        processor = systemInfo.getHardware().getProcessor();
        prevTicks = processor.getSystemCpuLoadTicks();
        memory = systemInfo.getHardware().getMemory();
    }

    public double cpuUsed() {
        try {
            return processor.getSystemCpuLoadBetweenTicks(prevTicks) * processor.getPhysicalProcessorCount();
        } finally {
            prevTicks = processor.getSystemCpuLoadTicks();
        }
    }

    public int cpuTotal() {
        return processor.getPhysicalProcessorCount();
    }

    public long memoryTotal() {
        return memory.getTotal();
    }

    public long memoryUsed() {
        return memory.getTotal() - memory.getAvailable();
    }
}
