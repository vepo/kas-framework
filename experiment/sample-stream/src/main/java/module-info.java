module maestro.experiment.sampleStream {
    requires transitive kafka.streams;
    requires java.management;
    requires kafka.clients;
    requires org.slf4j;
    requires com.fasterxml.jackson.databind;

    requires maestro.experiment.dataModel;

    exports io.vepo.kafka.stream.sample.serde to kafka.clients;
    exports io.vepo.kafka.stream.sample to com.fasterxml.jackson.databind;
}
