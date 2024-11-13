module maestro.experiment.sampleStream {
    requires transitive kafka.streams;
    requires kafka.clients;
    requires org.slf4j;
    requires com.fasterxml.jackson.databind;

    requires maestro.experiment.dataModel;
    requires maestro.kafkaStream;

    exports io.vepo.kafka.stream.sample.serde to kafka.clients;
    exports io.vepo.kafka.stream.sample to com.fasterxml.jackson.databind;
}
