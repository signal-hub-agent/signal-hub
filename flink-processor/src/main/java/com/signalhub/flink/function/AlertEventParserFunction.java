package com.signalhub.flink.function;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.signalhub.flink.entity.RawLog;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlertEventParserFunction extends RichFlatMapFunction<RawLog, String> {

    private static final Logger LOG = LoggerFactory.getLogger(AlertEventParserFunction.class);

    // ==========================================
    // 常量定义
    // ==========================================
    private static final String V2_SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
    private static final String V3_SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
    private static final String V2_MINT_TOPIC = "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f";
    private static final String V2_BURN_TOPIC = "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496";
    private static final String BRIDGE_TOPIC = "0xdc2e0b575a7c2fb2bc7c01bbf8a59b581be34d5ea4da62dc1030e5ea4e410a56";

    private static final List<String> STABLES = Arrays.asList("USDT", "USDC", "DAI", "USDE", "MUSD");
    private static final BigDecimal MNT_PRICE = new BigDecimal("0.85");

    private transient Map<String, Map<String, Object>> poolRegistry;
    private transient Set<String> smartMoneyAddresses;
    private transient Set<String> newlyDiscoveredPools; // 🌟 简化版零日防护网
    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) throws Exception {
        mapper = new ObjectMapper();
        newlyDiscoveredPools = new HashSet<>();

        // 1. 加载已知宇宙 (Registry)
        File registryFile = new File("/opt/signalhub/pool_registry_generated.json");
        if (registryFile.exists()) {
            poolRegistry = mapper.readValue(registryFile, new TypeReference<Map<String, Map<String, Object>>>() {});
            LOG.info("Loaded {} verified pools.", poolRegistry.size());
        }

        // 2. 加载聪明钱画像库
        File smartMoneyFile = new File("/opt/signalhub/smart_money_addresses.json");
        if (smartMoneyFile.exists()) {
            List<String> list = mapper.readValue(smartMoneyFile, new TypeReference<List<String>>() {});
            smartMoneyAddresses = new HashSet<>(list);
            LOG.info("Loaded {} smart money addresses.", smartMoneyAddresses.size());
        } else {
            smartMoneyAddresses = new HashSet<>();
        }
    }

    @Override
    public void flatMap(RawLog rawLog, Collector<String> out) {
        String topic0 = rawLog.getTopic0();
        if (topic0 == null) return;

        try {
            if (topic0.equals(V2_SWAP_TOPIC) || topic0.equals(V3_SWAP_TOPIC)) {
                processSwapEvent(rawLog, topic0, out);
            } else if (topic0.equals(V2_MINT_TOPIC) || topic0.equals(V2_BURN_TOPIC)) {
                processLiquidityEvent(rawLog, topic0, out);
            } else if (topic0.equals(BRIDGE_TOPIC)) {
                processBridgeEvent(rawLog, out);
            }
        } catch (Exception e) {
            LOG.error("Failed to parse event tx: {}", rawLog.getTxHash(), e);
        }
    }

    private void processSwapEvent(RawLog rawLog, String topic0, Collector<String> out) throws Exception {
        String poolAddress = rawLog.getContractAddress().toLowerCase();
        String traderAddress = "0x" + rawLog.getTopic2().substring(rawLog.getTopic2().length() - 40).toLowerCase();

        // ==========================================
        // 🌟 粗暴且高效的 Zero-Day 逻辑 (字典 Miss 判定)
        // ==========================================
        if (poolRegistry == null || !poolRegistry.containsKey(poolAddress)) {
            // 如果这个新池子在 Flink 运行期间还没报过警，就报警！
            if (!newlyDiscoveredPools.contains(poolAddress)) {
                newlyDiscoveredPools.add(poolAddress);

                // 为了兼容 Python 端的过滤条件，直接强制赋值 contract_age_hours = 0
                emitAlert(out, rawLog, traderAddress, "ZERO_DAY",
                        createDataNode("pool_address", poolAddress, "contract_age_hours", 0, "note", "New Unknown Pool Detected"));
            }
            // 因为字典里没有精度，没法算钱，报完 Zero-Day 就直接返回，放过后续逻辑
            return;
        }

        // ==========================================
        // 正常的 Whale 与 Smart Swap 逻辑 (在字典中)
        // ==========================================
        Map<String, Object> poolMeta = poolRegistry.get(poolAddress);
        ParsedData parsedData = parseHexData(rawLog.getData(), topic0,
                (String) poolMeta.get("t0_sym"), (String) poolMeta.get("t1_sym"),
                ((Number) poolMeta.get("t0_dec")).intValue(), ((Number) poolMeta.get("t1_dec")).intValue());

        if (!parsedData.isValid || parsedData.usdValue.compareTo(new BigDecimal("10")) < 0) return;

        // 🎯 告警 1: WHALE MOVEMENT
        if (parsedData.usdValue.compareTo(new BigDecimal("1000")) > 0) {
            emitAlert(out, rawLog, traderAddress, "WHALE_MOVEMENT",
                    createDataNode("usd_value", parsedData.usdValue, "token_symbol", parsedData.tokenOutSym, "flow_direction", "SWAP"));
        }

        // 🎯 告警 2: SMART SWAP
        if (smartMoneyAddresses.contains(traderAddress)) {
            emitAlert(out, rawLog, traderAddress, "SMART_SWAP",
                    createDataNode("usd_value", parsedData.usdValue, "token_symbol", parsedData.tokenOutSym, "dex_name", parsedData.dexName));
        }
    }

    private void processLiquidityEvent(RawLog rawLog, String topic0, Collector<String> out) throws Exception {
        String poolAddress = rawLog.getContractAddress().toLowerCase();
        if (poolRegistry == null || !poolRegistry.containsKey(poolAddress)) return;

        String action = topic0.equals(V2_MINT_TOPIC) ? "ADDED" : "REMOVED";
        Map<String, Object> poolMeta = poolRegistry.get(poolAddress);
        String poolName = poolMeta.get("t0_sym") + "-" + poolMeta.get("t1_sym");

        // 粗放估算金额
        ParsedData parsedData = parseHexData(rawLog.getData(), topic0,
                (String) poolMeta.get("t0_sym"), (String) poolMeta.get("t1_sym"),
                ((Number) poolMeta.get("t0_dec")).intValue(), ((Number) poolMeta.get("t1_dec")).intValue());

        if (parsedData.isValid && parsedData.usdValue.compareTo(new BigDecimal("5000")) > 0) {
            emitAlert(out, rawLog, poolAddress, "LIQUIDITY",
                    createDataNode("action", action, "pool_name", poolName, "usd_value", parsedData.usdValue));
        }
    }

    private void processBridgeEvent(RawLog rawLog, Collector<String> out) throws Exception {
        if (rawLog.getTopic1() == null) return;
        String userAddress = "0x" + rawLog.getTopic1().substring(rawLog.getTopic1().length() - 40).toLowerCase();

        String dataHex = rawLog.getData() != null ? rawLog.getData().replace("0x", "") : "";
        if (dataHex.length() >= 64) {
            BigDecimal amount = new BigDecimal(new BigInteger(dataHex.substring(0, 64), 16)).divide(BigDecimal.TEN.pow(18), 18, RoundingMode.HALF_UP);
            BigDecimal usdValue = amount.multiply(MNT_PRICE); // 简化逻辑：统统当做主网代币估算

            if (usdValue.compareTo(new BigDecimal("5000")) > 0) {
                emitAlert(out, rawLog, userAddress, "BRIDGE",
                        createDataNode("usd_value", usdValue, "bridge_direction", "Cross-Chain Transfer"));
            }
        }
    }

    // ==========================================
    // 万能十六进制解码与估值器
    // ==========================================
    private ParsedData parseHexData(String dataHex, String topic0, String t0Sym, String t1Sym, int t0Dec, int t1Dec) {
        ParsedData result = new ParsedData();
        result.isValid = false;
        result.usdValue = BigDecimal.ZERO;

        if (dataHex == null) return result;
        dataHex = dataHex.replace("0x", "");
        BigDecimal tokenInAmount = BigDecimal.ZERO;
        BigDecimal tokenOutAmount = BigDecimal.ZERO;

        try {
            if (topic0.equals(V2_SWAP_TOPIC) && dataHex.length() >= 256) {
                result.dexName = "merchant_moe";
                BigInteger a0In = new BigInteger(dataHex.substring(0, 64), 16);
                BigInteger a1In = new BigInteger(dataHex.substring(64, 128), 16);
                BigInteger a0Out = new BigInteger(dataHex.substring(128, 192), 16);
                BigInteger a1Out = new BigInteger(dataHex.substring(192, 256), 16);

                if (a0In.compareTo(BigInteger.ZERO) > 0 && a1Out.compareTo(BigInteger.ZERO) > 0) {
                    result.tokenInSym = t0Sym;
                    result.tokenOutSym = t1Sym;
                    tokenInAmount = new BigDecimal(a0In).divide(BigDecimal.TEN.pow(t0Dec), 18, RoundingMode.HALF_UP);
                    tokenOutAmount = new BigDecimal(a1Out).divide(BigDecimal.TEN.pow(t1Dec), 18, RoundingMode.HALF_UP);
                } else if (a1In.compareTo(BigInteger.ZERO) > 0 && a0Out.compareTo(BigInteger.ZERO) > 0) {
                    result.tokenInSym = t1Sym;
                    result.tokenOutSym = t0Sym;
                    tokenInAmount = new BigDecimal(a1In).divide(BigDecimal.TEN.pow(t1Dec), 18, RoundingMode.HALF_UP);
                    tokenOutAmount = new BigDecimal(a0Out).divide(BigDecimal.TEN.pow(t0Dec), 18, RoundingMode.HALF_UP);
                } else {
                    return result;
                }
            } else if (topic0.equals(V3_SWAP_TOPIC) && dataHex.length() >= 128) {
                result.dexName = "agni_finance";
                BigInteger amount0 = parseSignedInt256(dataHex.substring(0, 64));
                BigInteger amount1 = parseSignedInt256(dataHex.substring(64, 128));

                if (amount0.compareTo(BigInteger.ZERO) > 0 && amount1.compareTo(BigInteger.ZERO) < 0) {
                    result.tokenInSym = t0Sym;
                    result.tokenOutSym = t1Sym;
                    tokenInAmount = new BigDecimal(amount0).divide(BigDecimal.TEN.pow(t0Dec), 18, RoundingMode.HALF_UP);
                    tokenOutAmount = new BigDecimal(amount1.abs()).divide(BigDecimal.TEN.pow(t1Dec), 18, RoundingMode.HALF_UP);
                } else if (amount1.compareTo(BigInteger.ZERO) > 0 && amount0.compareTo(BigInteger.ZERO) < 0) {
                    result.tokenInSym = t1Sym;
                    result.tokenOutSym = t0Sym;
                    tokenInAmount = new BigDecimal(amount1).divide(BigDecimal.TEN.pow(t1Dec), 18, RoundingMode.HALF_UP);
                    tokenOutAmount = new BigDecimal(amount0.abs()).divide(BigDecimal.TEN.pow(t0Dec), 18, RoundingMode.HALF_UP);
                } else {
                    return result;
                }
            } else if ((topic0.equals(V2_MINT_TOPIC) || topic0.equals(V2_BURN_TOPIC)) && dataHex.length() >= 64) {
                result.dexName = "amm_pool";
                BigInteger a0 = new BigInteger(dataHex.substring(0, 64), 16);
                BigInteger a1 = dataHex.length() >= 128 ? new BigInteger(dataHex.substring(64, 128), 16) : BigInteger.ZERO;
                tokenInAmount = new BigDecimal(a0).divide(BigDecimal.TEN.pow(t0Dec), 18, RoundingMode.HALF_UP);
                tokenOutAmount = new BigDecimal(a1).divide(BigDecimal.TEN.pow(t1Dec), 18, RoundingMode.HALF_UP);
                result.tokenInSym = t0Sym;
                result.tokenOutSym = t1Sym;
            } else {
                return result;
            }

            String tIn = result.tokenInSym.toUpperCase();
            String tOut = result.tokenOutSym.toUpperCase();
            if (STABLES.contains(tIn)) result.usdValue = tokenInAmount;
            else if (STABLES.contains(tOut)) result.usdValue = tokenOutAmount;
            else if (tIn.equals("MNT") || tIn.equals("WMNT")) result.usdValue = tokenInAmount.multiply(MNT_PRICE);
            else if (tOut.equals("MNT") || tOut.equals("WMNT")) result.usdValue = tokenOutAmount.multiply(MNT_PRICE);

            result.isValid = true;

        } catch (Exception e) {
            LOG.error("Error parsing dataHex: {}", dataHex, e);
        }
        return result;
    }

    private BigInteger parseSignedInt256(String hexStr) {
        BigInteger val = new BigInteger(hexStr, 16);
        if (val.testBit(255)) {
            val = val.subtract(BigInteger.ONE.shiftLeft(256));
        }
        return val;
    }

    private void emitAlert(Collector<String> out, RawLog rawLog, String targetAddress, String eventType, ObjectNode dataNode) throws Exception {
        ObjectNode alertJson = mapper.createObjectNode();
        alertJson.put("event_id", rawLog.getTxHash() + "-" + eventType);
        alertJson.put("target_address", targetAddress);
        alertJson.put("event_type", eventType);
        alertJson.put("chain_name", rawLog.getChainName());
        alertJson.put("tx_hash", rawLog.getTxHash());
        alertJson.put("timestamp", rawLog.getBlockTimestamp());
        alertJson.set("data", dataNode);

        out.collect(mapper.writeValueAsString(alertJson));
    }

    private ObjectNode createDataNode(Object... keyValues) {
        ObjectNode node = mapper.createObjectNode();
        for (int i = 0; i < keyValues.length; i += 2) {
            String key = (String) keyValues[i];
            Object value = keyValues[i + 1];
            if (value instanceof BigDecimal) node.put(key, ((BigDecimal) value).doubleValue());
            else if (value instanceof Long) node.put(key, (Long) value);
            else if (value instanceof Integer) node.put(key, (Integer) value);
            else if (value instanceof String) node.put(key, (String) value);
        }
        return node;
    }

    private static class ParsedData {
        BigDecimal usdValue;
        String tokenInSym = "";
        String tokenOutSym = "";
        String dexName = "";
        boolean isValid;
    }
}