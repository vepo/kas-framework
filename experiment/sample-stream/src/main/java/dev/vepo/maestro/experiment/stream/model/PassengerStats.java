package dev.vepo.maestro.experiment.stream.model;

public record PassengerStats(long windowStart, long windowEnd, int totalTrips, double averagePassangerCount, double averageFare) {
}
