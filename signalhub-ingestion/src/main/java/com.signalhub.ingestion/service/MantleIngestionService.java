package com.signalhub.ingestion.service;

import com.signalhub.ingestion.entity.RawLog;
import com.signalhub.ingestion.producer.RawLogKafkaProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.EthLog;
import org.web3j.protocol.core.methods.response.Log;
import io.reactivex.disposables.Disposable;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Collections;
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

    private static final int BLOCK_TIMESTAMP_CACHE_SIZE = 1000;
    // Define the maximum chunk size for historical sync to avoid RPC 503 errors
    private static final long BLOCK_CHUNK_SIZE = 2000L;

    @Value("${app.mantle.sync-start-block:-1}")
    private long syncStartBlock;

    private final Web3j web3j;
    private final RawLogKafkaProducer kafkaProducer;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private Disposable subscription;

    private final Map<BigInteger, Long> blockTimestampCache = Collections.synchronizedMap(
            new LinkedHashMap<BigInteger, Long>(BLOCK_TIMESTAMP_CACHE_SIZE, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<BigInteger, Long> eldest) {
                    return size() > BLOCK_TIMESTAMP_CACHE_SIZE;
                }
            }
    );

    @PostConstruct
    public void startIngestion() {
        if (isRunning.compareAndSet(false, true)) {
            log.info("Starting Mantle log ingestion service.");

            // Start the ingestion process in a separate thread to avoid blocking Spring Boot startup
            new Thread(this::runIngestionPipeline, "Ingestion-Pipeline-Thread").start();
        }
    }

    private void runIngestionPipeline() {
        try {
            long currentLiveBlock = web3j.ethBlockNumber().send().getBlockNumber().longValue();
            long subscribeFromBlock = currentLiveBlock;

            if (syncStartBlock > 0 && syncStartBlock < currentLiveBlock) {
                log.info("Historical sync required. Target live block: {}. Starting from: {}", currentLiveBlock, syncStartBlock);
                subscribeFromBlock = performHistoricalSync(syncStartBlock, currentLiveBlock);
            }

            if (isRunning.get()) {
                subscribeToLiveEvents(subscribeFromBlock);
            }
        } catch (Exception e) {
            log.error("Fatal error in ingestion pipeline.", e);
            isRunning.set(false);
        }
    }

    /**
     * Fetches historical logs in chunks to prevent public RPC nodes from rejecting large block ranges.
     * Returns the next block number to start live subscription from.
     */
    private long performHistoricalSync(long startBlock, long targetBlock) {
        long currentStart = startBlock;

        while (currentStart <= targetBlock && isRunning.get()) {
            long currentEnd = Math.min(currentStart + BLOCK_CHUNK_SIZE - 1, targetBlock);
            log.info("Syncing historical chunk: {} to {}", currentStart, currentEnd);

            EthFilter filter = new EthFilter(
                    DefaultBlockParameter.valueOf(BigInteger.valueOf(currentStart)),
                    DefaultBlockParameter.valueOf(BigInteger.valueOf(currentEnd)),
                    (List<String>) null
            );
            filter.addOptionalTopics(V2_SWAP_TOPIC, V3_SWAP_TOPIC);

            try {
                EthLog ethLogResponse = web3j.ethGetLogs(filter).send();
                List<EthLog.LogResult> logs = ethLogResponse.getLogs();

                for (EthLog.LogResult logResult : logs) {
                    if (logResult.get() instanceof Log) {
                        processLog((Log) logResult.get());
                    }
                }

                log.info("Successfully synced chunk {} to {}. Processed {} events.", currentStart, currentEnd, logs.size());
            } catch (Exception e) {
                log.error("Failed to fetch historical chunk {} to {}. Retrying in 2 seconds...", currentStart, currentEnd, e);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                continue; // Retry the same chunk
            }

            currentStart = currentEnd + 1;
        }
        return currentStart;
    }

    private void subscribeToLiveEvents(long startBlock) {
        log.info("Configuring live subscription from block: {}", startBlock);

        EthFilter filter = new EthFilter(
                DefaultBlockParameter.valueOf(BigInteger.valueOf(startBlock)),
                DefaultBlockParameterName.LATEST,
                (List<String>) null
        );
        filter.addOptionalTopics(V2_SWAP_TOPIC, V3_SWAP_TOPIC);

        subscription = web3j.ethLogFlowable(filter).subscribe(
                this::processLog,
                error -> {
                    log.error("Error occurred in live log subscription flow.", error);
                    isRunning.set(false);
                }
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

    @PreDestroy
    public void stopIngestion() {
        log.info("Stopping Mantle log ingestion service.");
        isRunning.set(false);
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
        }
        web3j.shutdown();
    }
}