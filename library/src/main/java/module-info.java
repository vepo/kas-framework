module maestro.kafkaStream {
    requires transitive kafka.streams;
    requires transitive kafka.clients;
    requires org.slf4j;
    requires java.management;
    requires com.github.oshi;

    exports dev.vepo.kafka.maestro.metrics;
    exports dev.vepo.kafka.maestro;
    exports dev.vepo.kafka.maestro.adapter to kafka.clients;
}
