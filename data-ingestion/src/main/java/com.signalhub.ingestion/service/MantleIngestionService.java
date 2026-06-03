package com.signalhub.ingestion.service;

import com.signalhub.ingestion.entity.RawLog;
import com.signalhub.ingestion.producer.RawLogKafkaProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Log;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
@RequiredArgsConstructor
public class MantleIngestionService {

    private static final String CHAIN_NAME = "mantle";
    private static final String V2_SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
    private static final String V3_SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
    private static final int BLOCK_TIMESTAMP_CACHE_SIZE = 100;

    private final Web3j web3j;
    private final RawLogKafkaProducer kafkaProducer;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private final Map<BigInteger, Long> blockTimestampCache = new LinkedHashMap<BigInteger, Long>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<BigInteger, Long> eldest) {
            return size() > BLOCK_TIMESTAMP_CACHE_SIZE;
        }
    };

    @PostConstruct
    public void startIngestion() {
        if (isRunning.compareAndSet(false, true)) {
            log.info("Starting Mantle log ingestion service.");
            subscribeToSwapEvents();
        }
    }

    @PreDestroy
    public void stopIngestion() {
        log.info("Stopping Mantle log ingestion service.");
        isRunning.set(false);
        web3j.shutdown();
    }

    private void subscribeToSwapEvents() {
        EthFilter filter = new EthFilter(
                DefaultBlockParameterName.LATEST,
                DefaultBlockParameterName.LATEST,
                (List<String>) null
        );

        filter.addOptionalTopics(V2_SWAP_TOPIC, V3_SWAP_TOPIC);

        web3j.ethLogFlowable(filter).subscribe(
                this::processLog,
                error -> log.error("Error occurred in log subscription flow.", error)
        );
    }

    private void processLog(Log ethLog) {
        if (!isRunning.get()) {
            return;
        }

        try {
            Long blockTimestamp = resolveBlockTimestamp(ethLog.getBlockNumber());

            RawLog rawLog = RawLog.builder()
                    .chainName(CHAIN_NAME)
                    .blockNumber(ethLog.getBlockNumber().longValue())
                    .blockTimestamp(blockTimestamp)
                    .txHash(ethLog.getTransactionHash())
                    .logIndex(ethLog.getLogIndex().longValue())
                    .contractAddress(ethLog.getAddress().toLowerCase())
                    .topic0(getTopicSafely(ethLog, 0))
                    .topic1(getTopicSafely(ethLog, 1))
                    .topic2(getTopicSafely(ethLog, 2))
                    .topic3(getTopicSafely(ethLog, 3))
                    .data(ethLog.getData())
                    .ingestionTimestamp(System.currentTimeMillis())
                    .build();

            kafkaProducer.send(rawLog);

        } catch (Exception e) {
            log.error("Failed to process log entry for transaction {}", ethLog.getTransactionHash(), e);
        }
    }

    private Long resolveBlockTimestamp(BigInteger blockNumber) {
        return blockTimestampCache.computeIfAbsent(blockNumber, k -> {
            try {
                EthBlock block = web3j.ethGetBlockByNumber(
                        org.web3j.protocol.core.DefaultBlockParameter.valueOf(blockNumber),
                        false
                ).send();

                return Optional.ofNullable(block.getBlock())
                        .map(b -> b.getTimestamp().longValue() * 1000)
                        .orElseGet(() -> {
                            log.warn("Block {} returned null during timestamp resolution.", blockNumber);
                            return System.currentTimeMillis();
                        });
            } catch (IOException e) {
                log.error("IO error while resolving timestamp for block {}", blockNumber, e);
                return System.currentTimeMillis();
            }
        });
    }

    private String getTopicSafely(Log ethLog, int index) {
        List<String> topics = ethLog.getTopics();
        if (topics != null && topics.size() > index) {
            return topics.get(index);
        }
        return null;
    }
}