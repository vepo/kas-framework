package dev.vepo.kafka.maestro.adapter;

import java.util.HashMap;
import java.util.Map;

public interface AdapterRule {

    public class RequiredChanges {
        private boolean increaseThread;
        private Map<String, Object> properties;
        private boolean change;

        public RequiredChanges() {
            this.change = false;
            this.increaseThread = false;
            this.properties = new HashMap<>();
        }

        public boolean changeRequired() {
            return change;
        }

        public boolean shouldIncreaseThreads() {
            return increaseThread;
        }

        public void increaseThread() {
            this.increaseThread = true;
        }

        public boolean useNewConfigs() {
            return !this.properties.isEmpty();
        }

        public Map<String, Object> newConfigs() {
            return this.properties;
        }

        @Override
        public String toString() {
            return String.format("RequiredChanges[change=%b, increaseThread=%b, properties=%s]", change, increaseThread,
                    properties);
        }
    }

    StreamsContext evaluate(StreamsContext context, RequiredChanges changes);
}
