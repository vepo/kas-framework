package dev.vepo.maestro.experiment.stream.model;

import java.time.Instant;

public record TipStatus(long windowStart, long windowEnd, int totalTrips, int lowTipCounter, int highTipCounter,
        int negativeTipCounter, int noTipCounter) {

    public static TipStatus initializer() {
        return new TipStatus(Instant.now().toEpochMilli(),
                Instant.now().toEpochMilli(),
                0,
                0,
                0,
                0,
                0);
    }

    public TipStatus add(TaxiTrip trip) {
        var tipPercentualValue = (trip.tipAmount() / trip.fareAmount()) * 100;
        return new TipStatus(windowStart,
                             windowEnd,
                             totalTrips + 1,
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
}
