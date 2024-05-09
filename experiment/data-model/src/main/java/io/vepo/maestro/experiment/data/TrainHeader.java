package io.vepo.maestro.experiment.data;

import com.fasterxml.jackson.annotation.JsonProperty;

public record TrainHeader(@JsonProperty("msg_type") String msgType,
                          @JsonProperty("msg_queue_timestamp") String msgQueueTimestamp,
                          @JsonProperty("user_id") String userId,
                          @JsonProperty("source_dev_id") String sourceDevId,
                          @JsonProperty("source_system_id") String sourceSystemId,
                          @JsonProperty("original_data_source") String originalDataSource) {

}
