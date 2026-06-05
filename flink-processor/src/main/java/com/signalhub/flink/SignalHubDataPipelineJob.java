package com.signalhub.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.signalhub.flink.entity.RawLog;
import com.signalhub.flink.sink.ClickHouseSinkFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main Flink job for SignalHub data ingestion.
 * Consumes raw logs from Kafka and sinks them into ClickHouse ODS.
 */
public class SignalHubDataPipelineJob {

    private static final Logger LOG = LoggerFactory.getLogger(SignalHubDataPipelineJob.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        LOG.info("Initializing SignalHub Flink pipeline environment.");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // Configure checkpointing for fault tolerance (60 seconds)
        env.enableCheckpointing(60000);

        String brokers = "localhost:9092";
        String topic = "signal-hub-raw-logs";
        String consumerGroup = "flink-ods-consumer-group";

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .uid("kafka-source");

        SingleOutputStreamOperator<RawLog> rawLogStream = kafkaStream.map(json -> {
                    try {
                        return OBJECT_MAPPER.readValue(json, RawLog.class);
                    } catch (Exception e) {
                        LOG.error("Failed to deserialize Kafka message: {}", json, e);
                        return null;
                    }
                })
                .name("Deserialize JSON")
                .uid("deserialize-json")
                .filter(log -> log != null)
                .name("Filter Nulls")
                .uid("filter-nulls");

        String chUrl = "jdbc:clickhouse://localhost:8123/signal_hub?socket_timeout=60000&connection_timeout=60000";

        rawLogStream.addSink(ClickHouseSinkFactory.createRawLogSink(chUrl, "default", ""))
                .name("ClickHouse ODS Sink")
                .uid("clickhouse-sink");

        LOG.info("Executing SignalHub Flink pipeline.");
        env.execute("SignalHub RawLog Ingestion Job");
    }
}