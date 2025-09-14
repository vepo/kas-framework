package dev.vepo.maestro.experiment.stream;

public enum Topics {
    NYC_TAXI_TRIPS("nyc-taxi-trips"),
    NYC_TAXI_DASHBOARD_FARE("nyc-taxi-dashboard-fare"), 
    NYC_TAXI_TRIPS_BY_PU_LOCATION_ID("nyc-taxi-trips-by-pu-location-id"), 
    NYC_TAXI_FARES_STORE("nyc-taxi-fares-store"), 
    NYC_TAXI_TIPS_STORE("nyc-taxi-tips-store"),
    NYC_TAXI_DASHBOARD_TIPS("nyc-taxi-dashboard-tips");

    private String name;

    private Topics(String name) {
        this.name = name;
    }

    public String topicName() {
        return name;
    }
}
