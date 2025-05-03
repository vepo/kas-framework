package dev.vepo.maestro.experiment.stream;

public enum Topics {
    VEHICLE_MOVIMENT("vehicle.moviment"),
    VEHICLE_INFO_STORE("vehicle.info.store"),
    VEHICLE_STATS("vehicle.stats");

    private String name;

    private Topics(String name) {
        this.name = name;
    }

    public String topicName() {
        return name;
    }
}
