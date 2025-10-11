package dev.vepo.maestro.experiment.data.generator;

import java.util.Random;

public class RawDataSupplier {
    private final Random random;
    private final byte[] data;
    
    public RawDataSupplier() {
        this.random = new Random();
        this.data = new byte[Integer.parseInt(System.getenv("MESSAGE_SIZE"))];
    }
    
    public byte[] next() {
        random.nextBytes(data);
        return data;
    }
}