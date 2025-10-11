package dev.vepo.maestro.experiment.stream.model;

public record FareStats(long windowStart, long windowEnd, int totalTrips, double totalRevenue, double averageFare) {
}
