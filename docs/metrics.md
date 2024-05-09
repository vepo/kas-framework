# Kafka Stream Metrics

## Design

Kafka Metrics Reporter works as a reference registry. Kafka Stream will register a metrics reference that will be updated by the Stream. Kafka Stream does not send the new metric values to the Reporter, it updates the registered reference object. So a Reporter should get the current value on Metrics at a fixed frequency.

The Kafka Metrics uses the following data types for Metrics: 
* Double
* Integer
* KafkaStreams.State
* Long
* String

## Kafka Admin

| Metric Name | Description | Importance |
|-------------|-------------|------------|
| commit-id | | |
| connection-close-rate | Connections closed per second in the window. Consumer global connection metric. | Medium |
| connection-close-total | | Medium |
| connection-count | The current number of active connections. Consumer global connection metric. | Medium |
| connection-creation-rate | New connections established per second in the window. Consumer global connection metric. | Medium |
| connection-creation-total | | Medium |
| count | | |
| failed-authentication-rate | | |
| failed-authentication-total | | |
| failed-reauthentication-rate | | |
| failed-reauthentication-total | | |
| incoming-byte-rate | | |
| incoming-byte-total | | |
| io-ratio | The fraction of time the I/O thread spent doing I/O. Global connection metric. | High |
| io-time-ns-avg | The average length of time for I/O per select call in nanoseconds. Consumer global connection metric. | High |
| io-time-ns-total | | |
| io-wait-ratio | The fraction of time the I/O thread spent waiting. Consumer global connection metric. | High |
| io-wait-time-ns-avg | | |
| io-wait-time-ns-total | | |
| io-waittime-total | | |
| iotime-total | | |
| network-io-rate | | |
| network-io-total | | |
| outgoing-byte-rate | | |
| outgoing-byte-total | | |
| reauthentication-latency-avg | | |
| reauthentication-latency-max | | |
| request-latency-avg | | |
| request-latency-max | | |
| request-rate | | |
| request-size-avg | | |
| request-size-max | | |
| request-total | | |
| response-rate | | |
| response-total | | |
| select-rate | | |
| select-total | | |
| start-time-ms | | |
| successful-authentication-no-reauth-total | | |
| successful-authentication-rate | | |
| successful-authentication-total | | |
| successful-reauthentication-rate | | |
| successful-reauthentication-total | | |
| version | | |


## Kafka Consumer

| Metric Name | Description | Importance |
|-------------|-------------|------------|
| assigned-partitions | | |
| bytes-consumed-rate | The average number of bytes consumed per second. | Highest |
| bytes-consumed-total | | |
| commit-id | | |
| commit-latency-avg | | |
| commit-latency-max | | |
| commit-rate | | |
| commit-sync-time-ns-total | | |
| commit-total | | |
| committed-time-ns-total | | |
| connection-close-rate | | |
| connection-close-total | | |
| connection-count | | |
| connection-creation-rate | | |
| connection-creation-total | | |
| count | | |
| failed-authentication-rate | | |
| failed-authentication-total | | |
| failed-reauthentication-rate | | |
| failed-reauthentication-total | | |
| failed-rebalance-rate-per-hour | | |
| failed-rebalance-total | | |
| fetch-latency-avg | | |
| fetch-latency-max | | |
| fetch-rate | The number of fetch requests per second. | Highest |
| fetch-size-avg | | |
| fetch-size-max | | |
| fetch-throttle-time-avg | The average throttle time in milliseconds. When quotas are enabled, the broker may delay fetch requests in order to throttle a consumer which has exceeded its limit. This metric indicates how throttling time has been added to fetch requests on average. | |
| fetch-throttle-time-max | | |
| fetch-total | | |
| heartbeat-rate | The average number of heartbeats per second. After a rebalance, the consumer sends heartbeats to the coordinator to keep itself active in the group. You can control this using the heartbeat.interval.ms setting for the consumer. You may see a lower rate than configured if the processing loop is taking more time to handle message batches. Usually this is OK as long as you see no increase in the join rate. | Highest |
| heartbeat-response-time-max | | |
| heartbeat-total | | |
| incoming-byte-rate | | |
| incoming-byte-total | | |
| io-ratio | | |
| io-time-ns-avg | | |
| io-time-ns-total | | |
| io-wait-ratio | | |
| io-wait-time-ns-avg | | |
| io-wait-time-ns-total | | |
| io-waittime-total | | |
| iotime-total | | |
| join-rate | | |
| join-time-avg | | |
| join-time-max | | |
| join-total | | |
| last-heartbeat-seconds-ago | The number of seconds since the last controller heartbeat. | Highest |
| last-poll-seconds-ago | The number of seconds since the last poll() invocation. Kafka 2.4.0 and later. | |
| last-rebalance-seconds-ago | | |
| network-io-rate | | |
| network-io-total | | |
| outgoing-byte-rate | | |
| outgoing-byte-total | | |
| partition-assigned-latency-avg | | |
| partition-assigned-latency-max | | |
| partition-lost-latency-avg | | |
| partition-lost-latency-max | | |
| partition-revoked-latency-avg | | |
| partition-revoked-latency-max | | |
| poll-idle-ratio-avg | | |
| preferred-read-replica | | |
| reauthentication-latency-avg | | |
| reauthentication-latency-max | | |
| rebalance-latency-avg | | |
| rebalance-latency-max | | |
| rebalance-latency-total | | |
| rebalance-rate-per-hour | | |
| rebalance-total | | |
| records-consumed-rate | The average number of records consumed per second. | Highest |
| records-consumed-total | | |
| records-lag-max | | |
| records-lead-min | | |
| records-per-request-avg | | |
| request-latency-avg | | |
| request-latency-max | | |
| request-rate | | |
| request-size-avg | | |
| request-size-max | | |
| request-total | | |
| response-rate | | |
| response-total | | |
| select-rate | | |
| select-total | | |
| start-time-ms | | |
| successful-authentication-no-reauth-total | | |
| successful-authentication-rate | | |
| successful-authentication-total | | |
| successful-reauthentication-rate | | |
| successful-reauthentication-total | | |
| sync-rate | | |
| sync-time-avg | | |
| sync-time-max | | |
| sync-total | | |
| time-between-poll-avg | | |
| time-between-poll-max | | |
| version | | |


## Kafka Producer

| Metric Name | Description | Importance |
|-------------|-------------|------------|
| batch-size-avg | | |
| batch-size-max | | |
| batch-split-rate | | |
| batch-split-total | | |
| buffer-available-bytes | | |
| buffer-exhausted-rate | | |
| buffer-exhausted-total | | |
| buffer-total-bytes | | |
| bufferpool-wait-ratio | | |
| bufferpool-wait-time-ns-total | | |
| bufferpool-wait-time-total | | |
| commit-id | | |
| compression-rate-avg | | |
| connection-close-rate | | |
| connection-close-total | | |
| connection-count | | |
| connection-creation-rate | | |
| connection-creation-total | | |
| count | | |
| failed-authentication-rate | | |
| failed-authentication-total | | |
| failed-reauthentication-rate | | |
| failed-reauthentication-total | | |
| flush-time-ns-total | | |
| incoming-byte-rate | | |
| incoming-byte-total | | |
| io-ratio | | |
| io-time-ns-avg | | |
| io-time-ns-total | | |
| io-wait-ratio | | |
| io-wait-time-ns-avg | | |
| io-wait-time-ns-total | | |
| io-waittime-total | | |
| iotime-total | | |
| metadata-age | | |
| metadata-wait-time-ns-total | | |
| network-io-rate | | |
| network-io-total | | |
| outgoing-byte-rate | | |
| outgoing-byte-total | | |
| produce-throttle-time-avg | | |
| produce-throttle-time-max | | |
| reauthentication-latency-avg | | |
| reauthentication-latency-max | | |
| record-error-rate | | |
| record-error-total | | |
| record-queue-time-avg | | |
| record-queue-time-max | | |
| record-retry-rate | | |
| record-retry-total | | |
| record-send-rate | | |
| record-send-total | | |
| record-size-avg | | |
| record-size-max | | |
| records-per-request-avg | | |
| request-latency-avg | | |
| request-latency-max | | |
| request-rate | | |
| request-size-avg | | |
| request-size-max | | |
| request-total | | |
| requests-in-flight | | |
| response-rate | | |
| response-total | | |
| select-rate | | |
| select-total | | |
| start-time-ms | | |
| successful-authentication-no-reauth-total | | |
| successful-authentication-rate | | |
| successful-authentication-total | | |
| successful-reauthentication-rate | | |
| successful-reauthentication-total | | |
| txn-abort-time-ns-total | | |
| txn-begin-time-ns-total | | |
| txn-commit-time-ns-total | | |
| txn-init-time-ns-total | | |
| txn-send-offsets-time-ns-total | | |
| version | | |
| waiting-threads | | |

## Kafka Stream

| Metric Name | Description | Importance |
|-------------|-------------|------------|
| active-buffer-count | | |
| active-process-ratio | | |
| alive-stream-threads | | |
| all-latency-avg | | |
| all-latency-max | | |
| all-rate | | |
| application-id | | |
| background-errors | | |
| block-cache-capacity | | |
| block-cache-data-hit-ratio | | |
| block-cache-filter-hit-ratio | | |
| block-cache-index-hit-ratio | | |
| block-cache-pinned-usage | | |
| block-cache-usage | | |
| blocked-time-ns-total | | |
| bytes-consumed-total | | |
| bytes-produced-total | | |
| bytes-read-compaction-rate | | |
| bytes-read-rate | | |
| bytes-read-total | | |
| bytes-written-compaction-rate | | |
| bytes-written-rate | | |
| bytes-written-total | | |
| cache-size-bytes-total | | |
| commit-id | | |
| commit-latency-avg | | |
| commit-latency-max | | |
| commit-rate | | |
| commit-ratio | | |
| commit-total | | |
| compaction-pending | | |
| compaction-time-avg | | |
| compaction-time-max | | |
| compaction-time-min | | |
| count | | |
| cur-size-active-mem-table | | |
| cur-size-all-mem-tables | | |
| delete-latency-avg | | |
| delete-latency-max | | |
| delete-rate | | |
| dropped-records-rate | | |
| dropped-records-total | | |
| enforced-processing-rate | | |
| enforced-processing-total | | |
| estimate-num-keys | | |
| estimate-pending-compaction-bytes | | |
| estimate-table-readers-mem | | |
| failed-stream-threads | | |
| flush-latency-avg | | |
| flush-latency-max | | |
| flush-rate | | |
| get-latency-avg | | |
| get-latency-max | | |
| get-rate | | |
| hit-ratio-avg | | |
| hit-ratio-max | | |
| hit-ratio-min | | |
| live-sst-files-size | | |
| mem-table-flush-pending | | |
| memtable-bytes-flushed-rate | | |
| memtable-bytes-flushed-total | | |
| memtable-flush-time-avg | | |
| memtable-flush-time-max | | |
| memtable-flush-time-min | | |
| memtable-hit-ratio | | |
| num-deletes-active-mem-table | | |
| num-deletes-imm-mem-tables | | |
| num-entries-active-mem-table | | |
| num-entries-imm-mem-tables | | |
| num-immutable-mem-table | | |
| num-live-versions | | |
| num-running-compactions | | |
| num-running-flushes | | |
| number-file-errors-total | | |
| number-open-files | | |
| poll-latency-avg | | |
| poll-latency-max | | |
| poll-rate | | |
| poll-ratio | | |
| poll-records-avg | | |
| poll-records-max | | |
| poll-total | | |
| prefix-scan-latency-avg | | |
| prefix-scan-latency-max | | |
| prefix-scan-rate | | |
| process-latency-avg | | |
| process-latency-max | | |
| process-rate | | |
| process-ratio | | |
| process-records-avg | | |
| process-records-max | | |
| process-total | | |
| punctuate-latency-avg | | |
| punctuate-latency-max | | |
| punctuate-rate | | |
| punctuate-ratio | | |
| punctuate-total | | |
| put-all-latency-avg | | |
| put-all-latency-max | | |
| put-all-rate | | |
| put-if-absent-latency-avg | | |
| put-if-absent-latency-max | | |
| put-if-absent-rate | | |
| put-latency-avg | | |
| put-latency-max | | |
| put-rate | | |
| range-latency-avg | | |
| range-latency-max | | |
| range-rate | | |
| record-e2e-latency-avg | | |
| record-e2e-latency-max | | |
| record-e2e-latency-min | | |
| record-lateness-avg | | |
| record-lateness-max | | |
| records-consumed-total | | |
| records-produced-total | | |
| restore-latency-avg | | |
| restore-latency-max | | |
| restore-rate | | |
| restore-remaining-records-total | | |
| restore-total | | |
| size-all-mem-tables | | |
| state | | |
| task-closed-rate | | |
| task-closed-total | | |
| task-created-rate | | |
| task-created-total | | |
| thread-start-time | | |
| total-sst-files-size | | |
| version | | |
| write-stall-duration-avg | | |
| write-stall-duration-total | | |