package dev.vepo.maestro.experiment.stream.model;

import java.time.Instant;

public record TripStats(long windowStart, 
                        long windowEnd, 
                        int totalTrips, 
                        double totalRevenue, 
                        double averageFare,
                        int lowTipCounter, 
                        int highTipCounter,
                        int negativeTipCounter, 
                        int noTipCounter) {

    public static TripStats initializer() {
        return new TripStats(Instant.now().toEpochMilli(),
                            Instant.now().toEpochMilli(),
                            0,
                            0.0, 
                            0.0,
                            0,
                            0,
                            0,
                            0);
    }

    public TripStats add(TaxiTrip trip) {
        var tipPercentualValue = (trip.tipAmount() / trip.fareAmount()) * 100;
        return new TripStats(windowStart,
                             windowEnd,
                             totalTrips + 1,
                             totalRevenue + trip.totalAmount(),
                             ((averageFare * totalTrips) + trip.fareAmount()) / (totalTrips + 1),
                             lowTipCounter + (isLowTip(tipPercentualValue) ? 1 : 0),
                             highTipCounter + (isHighTip(tipPercentualValue) ? 1 : 0),
                             negativeTipCounter + (isNegativeTip(tipPercentualValue) ? 1 : 0),
                             noTipCounter + (isZero(trip.tipAmount()) ? 1 : 0));
    }

    private boolean isZero(double value) {
        return Math.abs(value) < 2 * Double.MIN_VALUE;
    }

    private boolean isLowTip(double tipPercentualValue) {
        return tipPercentualValue > 0 && tipPercentualValue < 2;
    }

    private boolean isHighTip(double tipPercentualValue) {
        return tipPercentualValue > 50;
    }

    private boolean isNegativeTip(double tipPercentualValue) {
        return !isZero(tipPercentualValue) && tipPercentualValue < 0;
    }

    public FareStats toFare() {
        return new FareStats(this.windowStart,
                            this.windowEnd,
                            this.totalTrips,
                            this.totalRevenue,
                            this.averageFare);
    }

    public TipStats toTip() {
        return new TipStats(this.windowStart,
                            this.windowEnd,
                            this.totalTrips,
                            this.lowTipCounter,
                            this.highTipCounter,
                            this.negativeTipCounter,
                            this.noTipCounter);
    }
    
}
