package dev.vepo.maestro.experiment.data.generator;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import java.time.Instant;

public class InstantToLongDeserializer extends JsonDeserializer<Long> {
    @Override
    public Long deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        var token = p.currentToken();
        if (token.isNumeric()) {
            return p.getLongValue();
        } else {
            String dateString = p.getText();
            return Instant.parse(dateString).toEpochMilli();
        }
    }
}