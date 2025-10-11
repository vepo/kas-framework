package dev.vepo.kafka.maestro;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.vepo.kafka.maestro.metrics.EnvironmentMetrics;

public class MaestroRocksDBConfigSetter implements RocksDBConfigSetter {
    private static final Logger logger = LoggerFactory.getLogger(MaestroRocksDBConfigSetter.class);
    private static final AtomicInteger databaseCounter = new AtomicInteger(0);
    private static final EnvironmentMetrics metrics = new EnvironmentMetrics();
    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        var counter = databaseCounter.incrementAndGet();
        logger.info("Opening rocksDB... open={} used writeBuffer={}", counter, formatMemory(options.writeBufferSize() * counter));
        logger.info("JVM Total memory: {}", formatMemory(Runtime.getRuntime().totalMemory()));
        logger.info("Container total: {}", formatMemory(metrics.memoryTotal()));
        logger.info("Container used: {}", formatMemory(metrics.memoryUsed()));
    }

    @Override
    public void close(String storeName, Options options) {        
        var counter = databaseCounter.decrementAndGet();
        logger.info("Closing rocksDB... open={} used writeBuffer={}", counter, formatMemory(options.writeBufferSize() * counter));
        options.close();
    }

    public static String formatMemory(long bytes) {
        if (bytes <= 0)
            return "";
        if (bytes < 1000)
            return bytes + " B";

        CharacterIterator ci = new StringCharacterIterator("kMGTPE");
        while (bytes >= 99_999) {
            bytes /= 1024;
            ci.next();
        }
        return String.format(Locale.getDefault(), "%.3f %cB", bytes / 1024.0, ci.current());
    }

}
