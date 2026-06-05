package com.signalhub.flink.sink;

import com.signalhub.flink.entity.RawLog;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Factory class for generating ClickHouse JDBC sinks.
 */
public class ClickHouseSinkFactory {

    private static final String INSERT_SQL =
            "INSERT INTO signal_hub.raw_logs " +
                    "(chain_name, block_number, block_timestamp, tx_hash, log_index, contract_address, " +
                    "topic0, topic1, topic2, topic3, data, ingestion_timestamp) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public static SinkFunction<RawLog> createRawLogSink(String jdbcUrl, String username, String password) {
        return JdbcSink.sink(
                INSERT_SQL,
                (statement, log) -> {
                    statement.setString(1, log.getChainName());
                    statement.setLong(2, log.getBlockNumber());
                    statement.setLong(3, log.getBlockTimestamp());
                    statement.setString(4, log.getTxHash());
                    statement.setLong(5, log.getLogIndex());
                    statement.setString(6, log.getContractAddress());
                    statement.setString(7, log.getTopic0() != null ? log.getTopic0() : "");
                    statement.setString(8, log.getTopic1() != null ? log.getTopic1() : "");
                    statement.setString(9, log.getTopic2() != null ? log.getTopic2() : "");
                    statement.setString(10, log.getTopic3() != null ? log.getTopic3() : "");
                    statement.setString(11, log.getData() != null ? log.getData() : "");
                    statement.setLong(12, log.getIngestionTimestamp());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(2000)
                        .withBatchIntervalMs(3000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(jdbcUrl)
                        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                        .withUsername(username)
                        .withPassword(password)
                        .build()
        );
    }
}