package com.signalhub.ingestion.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.signalhub.ingestion.entity.RawLog;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RawLogKafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic.raw-logs}")
    private String topic;

    public void send(RawLog rawLog) {
        try {
            String payload = objectMapper.writeValueAsString(rawLog);
            kafkaTemplate.send(topic, rawLog.getTxHash(), payload);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize RawLog object for transaction {}", rawLog.getTxHash(), e);
        } catch (Exception e) {
            log.error("Failed to send RawLog to Kafka topic {} for transaction {}", topic, rawLog.getTxHash(), e);
        }
    }
}