package com.signalhub.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.signalhub.flink.entity.RawLog;
import com.signalhub.flink.function.AlertEventParserFunction;
import com.signalhub.flink.sink.ClickHouseSinkFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SignalHubDataPipelineJob {

    private static final Logger LOG = LoggerFactory.getLogger(SignalHubDataPipelineJob.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        LOG.info("Initializing SignalHub Flink pipeline environment.");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 🌟 1. 启用 Checkpointing (每 60 秒触发一次)
        env.enableCheckpointing(60000);

        // 🌟 2. 【核心修复】设置 Checkpoint 的持久化路径！
        // 如果没有这一行，状态只会保存在内存中，重启就丢了。在 Docker 中可以映射此目录。
        env.getCheckpointConfig().setCheckpointStorage(new Path("file:///tmp/flink-checkpoints/signalhub"));

        // 🌟 3. 设置任务即使被手动 Cancel，也要保留最后的 Checkpoint 文件
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        String brokers = "localhost:9092"; // Docker 环境下可能是 kafka:29092
        String topic = "signal-hub-raw-logs";
        String consumerGroup = "flink-ods-consumer-group";

        // 🌟 4. 【核心修复】改变 Offset 初始化策略
        // committedOffsets 表示：优先从 Kafka Consumer Group 提交的进度开始读。
        // OffsetResetStrategy.EARLIEST 表示：如果这是全新启动（查不到进度），才从最老的数据开始读。
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST))
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
        // 1. 调用刚才写的转换算子，得到实时告警 JSON 字符串流
        DataStream<String> alertJsonStream = rawLogStream
                .flatMap(new AlertEventParserFunction())
                .name("Parse and Compute Alerts")
                .uid("parse-compute-alerts");

        // 2. 将结果写回 Kafka 的 `signal-hub-alerts` Topic 供下游 Python 服务消费
        KafkaSink<String> kafkaAlertSink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers) // 复用上面的 brokers 变量
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("signal-hub-alerts")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        alertJsonStream.sinkTo(kafkaAlertSink)
                .name("Kafka Alert Sink")
                .uid("kafka-alert-sink");

        LOG.info("Executing SignalHub Flink pipeline.");
        env.execute("SignalHub RawLog Ingestion Job");
    }
}