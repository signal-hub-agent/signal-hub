package com.signalforge.flink.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents the raw log event consumed from Kafka.
 * Fields must align with the upstream Ingestion module.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class RawLog {
    private String chainName;
    private Long blockNumber;
    private Long blockTimestamp;
    private String txHash;
    private Long logIndex;
    private String contractAddress;
    private String topic0;
    private String topic1;
    private String topic2;
    private String topic3;
    private String data;
    private Long ingestionTimestamp;
}