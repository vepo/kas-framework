module maestro.experiment.dataGenerator {
    requires kafka.clients;
    requires org.slf4j;
    requires transitive com.fasterxml.jackson.databind;
    requires io.github.bucket4j.core;

    exports dev.vepo.maestro.experiment.data.generator;
}
