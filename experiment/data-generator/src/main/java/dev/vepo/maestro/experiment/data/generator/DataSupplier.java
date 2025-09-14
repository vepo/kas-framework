package dev.vepo.maestro.experiment.data.generator;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DataSupplier {

    private AtomicReference<Iterator<TaxiTrip>> currentIterator;
    private Supplier<Iterator<TaxiTrip>> iteratorSupplier;

    public DataSupplier(File dataset) {
        var mapper = new ObjectMapper();
        var factory = mapper.getFactory();
        this.currentIterator = new AtomicReference<>();
        this.iteratorSupplier = () -> {
            try {
                var parser = factory.createParser(dataset);
                // Ensure we start with an array
                if (parser.nextToken() != JsonToken.START_ARRAY) {
                    parser.close();
                    throw new IllegalArgumentException("JSON file does not start with an array");
                }
                var stream = Stream.generate(() -> {
                    try {
                        var nextToken = parser.nextToken();
                        if (nextToken == JsonToken.START_OBJECT) {
                            return mapper.treeToValue(mapper.readTree(parser), TaxiTrip.class).shift();
                        } else if (nextToken == JsonToken.END_ARRAY) {
                            parser.close();
                            return null;
                        }
                        return null;
                    } catch (IOException ex) {
                        return handle(ex);
                    }
                }).takeWhile(obj -> obj != null);
                return stream.iterator();
            } catch (IOException ex) {
                return handle(ex);
            }
        };
    }

    private <T> T handle(Exception ex) {
                System.err.println("Error opening stream...");
                ex.printStackTrace();
                System.exit(-1);
                return null;
    }

    public TaxiTrip next() {
        return this.currentIterator.updateAndGet(prev -> {
            if (Objects.isNull(prev) || !prev.hasNext()) {
                return this.iteratorSupplier.get();
            } else {
                return prev;
            }
        }).next();
    }
}
