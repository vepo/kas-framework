module maestro.experiment.dataGenerator {
    requires kafka.clients;
    requires org.slf4j;
    requires info.picocli;
    requires com.fasterxml.jackson.databind;
    requires io.github.bucket4j.core;
    requires stomp4j;

    exports dev.vepo.maestro.experiment.data.generator;
}
