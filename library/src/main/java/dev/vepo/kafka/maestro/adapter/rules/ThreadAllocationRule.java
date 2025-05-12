package dev.vepo.kafka.maestro.adapter.rules;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.vepo.kafka.maestro.adapter.context.ResourcesState;
import dev.vepo.kafka.maestro.adapter.context.StreamsContext;
import dev.vepo.kafka.maestro.adapter.context.ThroughputState;

public class ThreadAllocationRule implements AdapterRule {
    private static final Logger logger = LoggerFactory.getLogger(ThreadAllocationRule.class);

    @Override
    public StreamsContext evaluate(StreamsContext context, RequiredChanges changes) {
        logger.info("Evaluating rule... throughput={}", context.throughput());
        if(context.throughput() == ThroughputState.UNSUSTAINABLE) {
            var activeThreads = context.threadNumber();
            var averageCpuUsage = context.cpuUsage();
            var averageMemoryUsage = context.memoryUsage();
            logger.info("Calculating max threads... cpuUsage={} memoryUsage={} activeThreads={}", averageCpuUsage, averageMemoryUsage, activeThreads);
            var cpuPerThread = averageCpuUsage / activeThreads;
            var memoryPerThread = averageMemoryUsage / activeThreads;
            logger.info("Calculating max threads... cpuPerThread={} memoryPerThread={}", cpuPerThread, memoryPerThread);
            int maxThreadsCpu = (int) Math.floor(context.cpuAvailable() / cpuPerThread);
            int maxThreadMemory = (int) Math.floor(context.memoryAvailable() / memoryPerThread);
            logger.info("Calculating max threads... maxThreadsCpu={} maxThreadMemory={}", maxThreadsCpu, maxThreadMemory);
            int maxPossibleThreads = Math.min(Math.min(maxThreadsCpu, maxThreadMemory), context.totalPartitions());
            logger.info("Math done! maxThreads={} activeThreads={}", maxPossibleThreads, activeThreads);
            if (maxPossibleThreads <= activeThreads) {
                logger.info("Resources saturated!");
                return context.withResources(ResourcesState.SATURATED);
            } else {
                logger.info("Resources available! Increasing threads from {} to {}", activeThreads, maxPossibleThreads);
                changes.increaseThread(maxPossibleThreads);
                return context.withResources(ResourcesState.AVAILABLE);
            }
        }
        return context;
    }
}
