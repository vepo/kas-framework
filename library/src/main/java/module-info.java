module maestro.kafkaStream {
    requires transitive kafka.streams;
    requires transitive kafka.clients;
    requires org.slf4j;
    requires java.management;

    exports dev.vepo.kafka.maestro.metrics;
}
