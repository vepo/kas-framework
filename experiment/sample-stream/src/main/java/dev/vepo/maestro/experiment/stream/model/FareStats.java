package dev.vepo.maestro.experiment.stream.model;

import java.time.Instant;

public record FareStats(long windowStart, long windowEnd, int totalTrips, double totalRevenue, double averageFare) {

    public FareStats add(TaxiTrip trip) {
        return new FareStats(windowStart,
                             Instant.now().toEpochMilli(), 
                             totalTrips + 1, 
                             totalRevenue + trip.totalAmount(),
                             ((averageFare * totalTrips) + trip.fareAmount()) / (totalTrips + 1));
    }

    public static FareStats initializer() {
        return new FareStats(Instant.now().toEpochMilli(), 
                             Instant.now().toEpochMilli(), 
                             0,
                             0.0, 
                             0.0);
    }
}
