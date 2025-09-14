package dev.vepo.maestro.experiment.stream.model;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * Sample data.
 * <pre>
 *   {
 *     "Airport_fee":0,
 *     "DOLocationID":237,
 *     "PULocationID":263,
 *     "RatecodeID":1,
 *     "VendorID":2,
 *     "cbd_congestion_fee":0.75,
 *     "congestion_surcharge":2.5,
 *     "extra":0,
 *     "fare_amount":7.9,
 *     "improvement_surcharge":1,
 *     "mta_tax":0.5,
 *     "passenger_count":2,
 *     "payment_type":1,
 *     "store_and_fwd_flag":"N",
 *     "tip_amount":1,
 *     "tolls_amount":0,
 *     "total_amount":13.65,
 *     "tpep_dropoff_datetime":1748764865000000,
 *     "tpep_pickup_datetime":1748764568000000,
 *     "trip_distance":1.39
 *   }
 * </pre>
 * @see <a href="https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf">Yellow Trips Data Dictionary</a>
 */
public record TaxiTrip(@JsonProperty("VendorID") int vendorId,
                       @JsonProperty("tpep_pickup_datetime") long pickupTimestamp,
                       @JsonProperty("tpep_dropoff_datetime") long dropTimestamp,
                       @JsonProperty("passenger_count") int passengerCount,
                       @JsonProperty("trip_distance") double tripDistance,
                       @JsonProperty("RatecodeID") int ratecodeID,
                       @JsonProperty("store_and_fwd_flag") String storeAndFwdFlag,
                       @JsonProperty("PULocationID") int puLocationID,
                       @JsonProperty("DOLocationID") int doLocationID,
                       @JsonProperty("payment_type") int paymentType,
                       @JsonProperty("fare_amount") double fareAmount,
                       @JsonProperty("extra") double extra,
                       @JsonProperty("mta_tax") double mtaTax,
                       @JsonProperty("tip_amount") double tipAmount,
                       @JsonProperty("tolls_amount") double tollsAmount,
                       @JsonProperty("improvement_surcharge") double improvementSurcharge,
                       @JsonProperty("total_amount") double totalAmount,
                       @JsonProperty("congestion_surcharge") double congestionSurcharge,
                       @JsonProperty("Airport_fee") double airportFee,
                       @JsonProperty("cbd_congestion_fee") double cbdCongestionFee) {

    private static final DateTimeFormatter hourlyFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
    private static final ZoneId nycZoneId = ZoneId.of("America/New_York");

    public String getHourlyKey() {
        Instant instant = Instant.ofEpochMilli(pickupTimestamp / 1000000);
        return instant.atZone(nycZoneId).format(hourlyFormatter);
    }
}
