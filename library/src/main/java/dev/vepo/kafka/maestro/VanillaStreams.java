package dev.vepo.kafka.maestro;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

public class VanillaStreams implements Streams {

    private final KafkaStreams innerStreams;

    public VanillaStreams(Topology topology, Properties props) {
        innerStreams = new KafkaStreams(topology, props);
    }

    @Override
    public void cleanUp() {
        this.innerStreams.cleanUp();
    }

    @Override
    public void start() {
        this.innerStreams.start();
    }

    @Override
    public void close() {
        this.innerStreams.close();
    }

}
