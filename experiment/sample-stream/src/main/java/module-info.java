module maestro.experiment.sampleStream {
    requires transitive kafka.streams;
    requires java.management;
    requires kafka.clients;
    requires org.slf4j;
    requires com.fasterxml.jackson.databind;
    requires maestro.kafkaStream;

    requires info.picocli;
}
