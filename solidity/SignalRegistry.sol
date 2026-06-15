// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

/**
 * @title SignalRegistry
 * @dev A dual-track on-chain registry for AI-driven Alpha signals and real-time alerts.
 * Designed for the Mantle AI Hackathon, compliant with the ERC-8004 Agent Identity standard.
 */
contract SignalRegistry {
    address public owner;
    uint256 public aiAgentId; // ERC-8004 Agent ID assigned by Mantle

    uint256 public alertBatchId;
    uint256 public insightBatchId;

    // ==========================================
    // TRACK 1: Real-time Transaction Alerts (High-frequency)
    // ==========================================
    struct AlertItem {
        string txHash;      // The transaction hash that triggered the alert
        address target;     // The smart money or monitored address
        string eventType;   // e.g., "SMART_SWAP", "ZERO_DAY"
    }

    event AlertBatchPublished(
        uint256 indexed batchId,
        uint256 indexed agentId,
        uint256 timestamp,
        uint256 totalAlerts,
        AlertItem[] alerts
    );

    // ==========================================
    // TRACK 2: AI Top Address Insights (Aggregated & Evaluated)
    // ==========================================
    struct TopAddressItem {
        address targetAddress; // Address identified by AI as high-value/smart money
        uint8 aiScore;         // Aggregated AI confidence score (0-100)
        string reason;         // Core reasoning e.g., "High Win-Rate in MEME", "Whale Accumulation"
    }

    event TopAddressesPublished(
        uint256 indexed batchId,
        uint256 indexed agentId,
        uint256 timestamp,
        uint256 totalAddresses,
        TopAddressItem[] topAddresses
    );

    // ==========================================
    // Core Contract Logic
    // ==========================================

    /**
     * @param _initialAgentId The initial ERC-8004 ID (can be 0 pending official registration)
     */
    constructor(uint256 _initialAgentId) {
        owner = msg.sender;
        aiAgentId = _initialAgentId;
    }

    modifier onlyOwner() {
        require(msg.sender == owner, "Only owner allowed");
        _;
    }

    /**
     * @dev Update the Agent ID once officially registered in the Mantle Ecosystem
     */
    function setAgentId(uint256 _newAgentId) external onlyOwner {
        aiAgentId = _newAgentId;
    }

    /**
     * @dev Publish a batch of real-time alerts to the Mantle network
     */
    function publishAlertBatch(AlertItem[] calldata _alerts) external onlyOwner {
        require(_alerts.length > 0, "Empty alert batch");
        alertBatchId++;
        emit AlertBatchPublished(alertBatchId, aiAgentId, block.timestamp, _alerts.length, _alerts);
    }

    /**
     * @dev Publish a batch of AI-evaluated top addresses and their scores
     */
    function publishTopAddresses(TopAddressItem[] calldata _topAddresses) external onlyOwner {
        require(_topAddresses.length > 0, "Empty insight batch");
        insightBatchId++;
        emit TopAddressesPublished(insightBatchId, aiAgentId, block.timestamp, _topAddresses.length, _topAddresses);
    }
}