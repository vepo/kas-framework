package dev.vepo.kafka.maestro.adapter;

public class ThreadAllocationRule implements AdapterRule {

    @Override
    public StreamsContext evaluate(StreamsContext context, RequiredChanges changes) {
        if(context.throughput() == ThroughputState.UNSUSTAINABLE) {
            var activeThreads = context.threadNumber();
            var averageCpuUsage = context.cpuUsage();
            var averageMemoryUsage = context.memoryUsage();

            var cpuPerThread = averageCpuUsage / activeThreads;
            var memoryPerThread = averageMemoryUsage / activeThreads;
            var maxThreadsCpu = Math.floor(context.cpuAvailable() / cpuPerThread);
            var maxThreadMemory = Math.floor(context.memoryAvailable() / memoryPerThread);
            var maxPossibleThreads = Math.max(maxThreadsCpu, maxThreadMemory);
            if (maxPossibleThreads <= activeThreads) {
                changes.increaseThread();
                return context.withResources(ResourcesState.SATURATED);
            } else {
                return context.withResources(ResourcesState.AVAILABLE);
            }
        }
        return context;
    }
}
