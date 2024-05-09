module maestro.kafkaStream {
    requires transitive kafka.streams;
    requires transitive kafka.clients;
    requires org.slf4j;

    exports io.vepo.kafka.maestro;
    exports io.vepo.kafka.maestro.metrics;
}
