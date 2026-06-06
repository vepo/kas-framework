package dev.vepo.adaptive.experiment.stream.model;

public record PassengerStats(long windowStart, long windowEnd, int totalTrips, double averagePassangerCount,
                             double averageFare, double averageDistance, double averageDuration) {}
