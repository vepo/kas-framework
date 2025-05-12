package dev.vepo.kafka.maestro.adapter.rules;

import java.util.HashMap;
import java.util.Map;

import dev.vepo.kafka.maestro.adapter.context.StreamsContext;

public interface AdapterRule {

    public class RequiredChanges {
        private int numThreads;;
        private Map<String, Object> properties;

        public RequiredChanges() {
            this.numThreads = 0;
            this.properties = new HashMap<>();
        }

        public boolean changeRequired() {
            return numThreads > 0 || this.properties.size() > 0;
        }

        public boolean shouldIncreaseThreads() {
            return numThreads > 0;
        }

        public void increaseThread(int numThreads) {
            this.numThreads = numThreads;
        }

        public int numThreads() {
            return numThreads;
        }

        public boolean useNewConfigs() {
            return !this.properties.isEmpty();
        }

        public void useValue(String key, Object value) {
            this.properties.put(key, value);
        }

        public Map<String, Object> newConfigs() {
            return this.properties;
        }

        @Override
        public String toString() {
            return String.format("RequiredChanges[numThreads=%d, properties=%s]", numThreads, properties);
        }
    }

    StreamsContext evaluate(StreamsContext context, RequiredChanges changes);
}
