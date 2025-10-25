package dev.vepo.maestro.experiment.stream;

public enum Topics {
    RAW_DATA_INPUT("raw-input"),
    RAW_DATA_OUTPUT("raw-output"),
    RAW_BY_HASH("raw-by-hash"),
    RAW_UNIQUE_STORE("raw-unique-store"),
    NYC_TAXI_TRIPS("nyc-taxi-trips"),
    NYC_TAXI_DASHBOARD_FARE("nyc-taxi-dashboard-fare"), 
    NYC_TAXI_TRIPS_BY_PU_LOCATION_ID("nyc-taxi-trips-by-pu-location-id"), 
    NYC_TAXI_TRIPS_BY_DO_LOCATION_ID("nyc-taxi-trips-by-do-location-id"), 
    NYC_TAXI_PU_STATS_STORE("nyc-taxi-pu-stats-store"), 
    NYC_TAXI_DO_STATS_STORE("nyc-taxi-do-stats-store"), 
    NYC_TAXI_DASHBOARD_TIPS("nyc-taxi-dashboard-tips"), 
    NYC_TAXI_DASHBOARD_PASSENGERS("nyc-taxi-dashboard-passengers");

    private String name;

    private Topics(String name) {
        this.name = name;
    }

    public String topicName() {
        return name;
    }
}
