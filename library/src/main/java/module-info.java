module kas.stream {
    requires transitive kafka.streams;
    requires transitive kafka.clients;
    requires transitive commons.math3;
    requires transitive rocksdbjni;
    requires org.slf4j;
    requires java.management;
    requires jdk.management;

    exports dev.vepo.kafka.adaptive.metrics;
    exports dev.vepo.kafka.adaptive;
    exports dev.vepo.kafka.adaptive.adapter.rules;
    exports dev.vepo.kafka.adaptive.adapter to kafka.clients;
}
