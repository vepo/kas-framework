package io.vepo.kafka.stream.sample.metrics;

import java.util.regex.Pattern;

public record StreamClientId(String id) {
    private static String CLIENT_ID_REGEX = ".*-StreamThread-(\\d+)-[a-z]+";
    private static Pattern CLIENT_ID_PATTERN = Pattern.compile(CLIENT_ID_REGEX);

    public int threadId() {
        var matcher = CLIENT_ID_PATTERN.matcher(id);
        if (matcher.matches()) {
            return Integer.parseInt(matcher.group(1));
        } else {
            throw new IllegalArgumentException("Invalid client id: " + id);
        }
    }
}
