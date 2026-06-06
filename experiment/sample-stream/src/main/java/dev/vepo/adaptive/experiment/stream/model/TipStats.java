package dev.vepo.adaptive.experiment.stream.model;

public record TipStats(long windowStart, long windowEnd, int totalTrips, int lowTipCounter, int highTipCounter,
                       int negativeTipCounter, int noTipCounter, double averageDistance, double averageDuration) {}
