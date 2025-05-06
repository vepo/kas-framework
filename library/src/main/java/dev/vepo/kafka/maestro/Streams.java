package dev.vepo.kafka.maestro;

public interface Streams extends AutoCloseable {

    void cleanUp();
    void start();
    void close();

}
