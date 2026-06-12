package com.signalhub.ingestion.service;

import com.signalhub.ingestion.entity.RawLog;
import com.signalhub.ingestion.producer.RawLogKafkaProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
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
    // 新增：流动性池 Mint (加池子) 和 Burn (撤池子) 的标准 Topic
    private static final String V2_MINT_TOPIC = "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f";
    private static final String V2_BURN_TOPIC = "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496";
    // 新增：跨链桥的标准 Deposit/Withdraw Topic (需根据 Mantle 官方桥 ABI 确认，此处用标准 ERC20 占位)
    private static final String BRIDGE_DEPOSIT_TOPIC = "0xdc2e0b575a7c2fb2bc7c01bbf8a59b581be34d5ea4da62dc1030e5ea4e410a56";
    private static final int BLOCK_TIMESTAMP_CACHE_SIZE = 1000;
    private static final long BLOCK_CHUNK_SIZE = 2000L;

    @Value("${app.mantle.sync-start-block:-1}")
    private long syncStartBlock;

    private final Web3j web3j;
    private final RawLogKafkaProducer kafkaProducer;
    private final JdbcTemplate jdbcTemplate; // 🌟 引入 PG 操作模板

    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private Disposable subscription;

    // 🌟 用于控制只在区块切换时写库，避免击穿 PG
    private volatile long lastSavedBlock = 0;

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
            new Thread(this::runIngestionPipeline, "Ingestion-Pipeline-Thread").start();
        }
    }

    private void runIngestionPipeline() {
        try {
            long currentLiveBlock = web3j.ethBlockNumber().send().getBlockNumber().longValue();
            long subscribeFromBlock;

            // 🌟 1. 优先从 PostgreSQL 读取上次断点
            Long dbLastBlock = getCheckpointFromPG();

            if (dbLastBlock != null) {
                subscribeFromBlock = dbLastBlock + 1;
                log.info("🚀 Resuming from PostgreSQL checkpoint. Starting block: {}", subscribeFromBlock);
                if (subscribeFromBlock < currentLiveBlock) {
                    subscribeFromBlock = performHistoricalSync(subscribeFromBlock, currentLiveBlock);
                }
            } else if (syncStartBlock > 0 && syncStartBlock < currentLiveBlock) {
                log.info("⚠️ No PostgreSQL checkpoint found. Using static syncStartBlock: {}", syncStartBlock);
                subscribeFromBlock = performHistoricalSync(syncStartBlock, currentLiveBlock);
            } else {
                log.info("🆕 No historical sync needed. Starting from live block: {}", currentLiveBlock);
                subscribeFromBlock = currentLiveBlock;
            }

            if (isRunning.get()) {
                subscribeToLiveEvents(subscribeFromBlock);
            }
        } catch (Exception e) {
            log.error("Fatal error in ingestion pipeline.", e);
            isRunning.set(false);
        }
    }

    private Long getCheckpointFromPG() {
        try {
            String sql = "SELECT last_processed_block FROM chain_sync_state WHERE chain_name = ?";
            return jdbcTemplate.queryForObject(sql, Long.class, CHAIN_NAME);
        } catch (EmptyResultDataAccessException e) {
            return null; // 表里没有记录
        } catch (Exception e) {
            log.error("Failed to read checkpoint from PostgreSQL.", e);
            return null;
        }
    }

    private void saveCheckpointToPG(long blockNumber) {
        try {
            // PostgreSQL 独有的 UPSERT (Insert or Update) 语法
            String sql = "INSERT INTO chain_sync_state (chain_name, last_processed_block, updated_at) " +
                    "VALUES (?, ?, NOW()) " +
                    "ON CONFLICT (chain_name) DO UPDATE " +
                    "SET last_processed_block = EXCLUDED.last_processed_block, updated_at = NOW()";
            jdbcTemplate.update(sql, CHAIN_NAME, blockNumber);
            lastSavedBlock = blockNumber;
        } catch (Exception e) {
            log.error("Failed to save checkpoint {} to PostgreSQL.", blockNumber, e);
        }
    }

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
            filter.addOptionalTopics(V2_SWAP_TOPIC, V3_SWAP_TOPIC, V2_MINT_TOPIC, V2_BURN_TOPIC, BRIDGE_DEPOSIT_TOPIC);
            try {
                EthLog ethLogResponse = web3j.ethGetLogs(filter).send();
                List<EthLog.LogResult> logs = ethLogResponse.getLogs();

                for (EthLog.LogResult logResult : logs) {
                    if (logResult.get() instanceof Log) {
                        processLog((Log) logResult.get());
                    }
                }

                // 🌟 Chunk 同步完成后，将当前 Chunk 的结尾区块写入 PG
                saveCheckpointToPG(currentEnd);
                log.info("Successfully synced chunk {} to {}. Processed {} events.", currentStart, currentEnd, logs.size());
            } catch (Exception e) {
                log.error("Failed to fetch historical chunk {} to {}. Retrying in 2 seconds...", currentStart, currentEnd, e);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                continue;
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

        // 如果之前有旧的订阅，先清理掉
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
        }

        subscription = web3j.ethLogFlowable(filter).subscribe(
                this::processLog,
                error -> {
                    log.error("❌ Live subscription flow broken (e.g. filter not found). Error: {}", error.getMessage());

                    // 🌟 核心修复：发生错误时，不是退出程序，而是等待几秒后重新订阅
                    if (isRunning.get()) {
                        log.info("🔄 Attempting to reconnect live subscription...");
                        try {
                            Thread.sleep(3000); // 稍微等一下，避免节点被频繁重试打挂
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }

                        // 从我们记录在内存中的最后一次保存的区块高度继续监听
                        // 如果没有，就退回到传入的 startBlock
                        long reconnectBlock = lastSavedBlock > 0 ? lastSavedBlock : startBlock;
                        log.info("🔄 Reconnecting from block: {}", reconnectBlock);

                        // 递归调用自己，创建一个全新的 Filter
                        subscribeToLiveEvents(reconnectBlock);
                    }
                }
        );
    }

    private void processLog(Log ethLog) {
        if (!isRunning.get()) {
            return;
        }

        try {
            long currentBlockNum = ethLog.getBlockNumber().longValue();
            Long blockTimestamp = resolveBlockTimestamp(ethLog.getBlockNumber());

            RawLog rawLog = RawLog.builder()
                    .chainName(CHAIN_NAME)
                    .blockNumber(currentBlockNum)
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

            // 发送给 Kafka
            kafkaProducer.send(rawLog);

            // 🌟 实时流模式下，只有当区块号发生变更时，才向 PG 写入一次（避免每个 Log 都写库）
            if (currentBlockNum > lastSavedBlock) {
                saveCheckpointToPG(currentBlockNum);
            }

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