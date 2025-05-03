package dev.vepo.maestro.experiment.stream.model;

public record VehicleInfo(String id, double latitude, double longitude, double speed,
                          double bearing, boolean accelerating, long timestamp) {

}
