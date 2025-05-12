module maestro.kafkaStream {
    requires transitive kafka.streams;
    requires transitive kafka.clients;
    requires transitive commons.math3;
    requires org.slf4j;
    requires java.management;
    requires jdk.management;

    exports dev.vepo.kafka.maestro.metrics;
    exports dev.vepo.kafka.maestro;
    exports dev.vepo.kafka.maestro.adapter to kafka.clients;
}
