# SignalHub

### AI-Driven Dual-Track Real-Time On-Chain Alert & Analytics Platform
> Designed for the Mantle AI Ecosystem, compliant with the ERC-8004 Agent Identity standard.

SignalHub is an end-to-end, stream-batch integrated intelligent on-chain data monitoring and AI insight platform. Utilizing a high-throughput distributed architecture, the system performs millisecond-level stream computing on native blockchain anomalies. Combined with Large Language Models (LLMs), it aggregates daily macro and micro on-chain behaviors, deeply analyzing Smart Money and high-potential tokens. Ultimately, it achieves **on-chain data consensus and verification (Integrity Monitoring System)** along with **personalized cross-platform (Web + Telegram) alert routing and dispatching**.

---

## 🚀 Core Features

### 1. Millisecond-Level High-Throughput Stream Alerts
* Monitors various on-chain high-risk and high-value behaviors: Whale Movements, Zero-Day Interactions (first-time contract interactions), Liquidity Provisioning (massive pool additions/removals), and large-scale Bridge Transfers.
* Powered by Apache Flink and Apache Kafka to ensure ultra-high throughput and sub-second end-to-end latency.

### 2. Dual-Track Blockchain Data Anchoring
* **Real-Time Alert Batching (AlertBatch):** High-frequency stream alert signals are batched via an efficient Redis-backed queueing mechanism, drastically reducing Gas costs before being written to the Mantle network.
* **AI Insights Anchoring (TopAddresses):** Daily batch-generated LLM evaluation data—including Smart Money win rates, Risk/Return modeling (PnL Ratio), and token attribution tags—are written to the chain through an independent dedicated channel.
* Supports full on-chain cryptographic traceability, fully compliant with the ERC-8004 Agent Identity specification.

### 3. Personalized Multi-Terminal Alert Routing Engine
* **Hybrid Web3/Web2 Authentication:** Seamless integration of Google OAuth (managed by Next-Auth) alongside native multi-chain Web3 wallet connectivity via RainbowKit and Wagmi.
* **Decentralized Custom Thresholds:** Users can dynamically define custom execution boundaries for various on-chain events (e.g., set minimum USD threshold for Whale Movements or maximum contract age for Zero-Day interactions).
* **Closed-Loop Telegram Alerts Terminal:** An innovative onboarding protocol utilizing time-sensitive security tokens (secured via Redis TTL locks) pairs Web3 addresses to Telegram `chat_id` profiles, enabling seamless Webhook-driven instant pushing.

### 4. Offline Pre-Computation & Performance Optimization
* Replaces heavy synchronous real-time LLM requests with automated offline cron batch processing, completely eliminating "first-user latency penalties" or LLM cold starts.
* Employs memory-grid caching via Redis alongside strict Pydantic v2 data structure validation, driving core API response times down to the millisecond level.

---

## 🛠️ Tech Stack

* **Frontend:** Next.js 14 (App Router), TypeScript, TailwindCSS, RainbowKit, Next-Auth, Lucide React
* **Application Gateway (Backend API):** FastAPI (Python 3.11+), Pydantic v2, PostgreSQL (Asyncpg)
* **Streaming & Message Queue:** Apache Flink, Apache Kafka, Redis (Data Buffering & Caching)
* **Web3 & Blockchain:** Web3.py (AsyncWeb3), Solidity (v0.8.19), Mantle Sepolia Testnet
* **AI Inference Engine:** Structured JSON LLM Inference Engine (Batch-optimized Prompts)

---

## 📂 Repository Structure

```text
├── signal-hub-web/             # Frontend Next.js Project
│   ├── src/
│   │   ├── app/                # App Router main entries
│   │   │   ├── alerts/         # Alert rules configuration engine page
│   │   │   ├── api/            # Next.js edge API routes
│   │   │   └── providers.tsx   # Global Web3/Auth context provider
│   │   ├── components/
│   │   │   ├── ui/             # Atomic UI components
│   │   │   │   └── TelegramBindCard.tsx # Telegram binding card component
│   │   │   └── Navbar.tsx      # Multi-modal global navigation bar
│   │   └── hooks/              # Custom React Hooks collection
│   └── package.json
│
├── backend-api/                # Backend FastAPI Project
│   ├── api/
│   │   ├── bot/                # Telegram binding & Webhook routing
│   │   │   ├── router.py
│   │   │   ├── service.py
│   │   │   └── schemas.py
│   │   └── dashboard/          # Global KPI matrix & metrics services
│   ├── core/
│   │   ├── db_postgres.py      # Async PostgreSQL pooling utility
│   │   ├── redis_client.py     # Async Redis memory grid client
│   │   └── config.py           # Environment variables & global configs
│   └── cron_daily_ai_job.py    # Daily midnight batch pre-computation cron task
│
└── blockchain-worker/          # Blockchain anchoring services & smart contracts
    ├── contracts/
    │   └── SignalRegistry.sol  # Solidity Contract (Dual-track events & identity mapping)
    └── batcher.py              # Distributed flow-controlled batching daemon
```

---

## ⚙️ Environment Configuration

### 1. Backend & Worker Setup
Create a `.env` file under the `backend-api/` and `blockchain-worker/` directories:
```env
# Core Services
KAFKA_BROKERS=localhost:9092
REDIS_URL=redis://localhost:6379/0
POSTGRES_URL=postgresql://user:password@localhost:5432/signal_hub

# Smart Contract Configurations
PUBLISHER_PRIVATE_KEY=0x_your_mantle_testnet_private_key_
CONTRACT_ADDRESS=0x_deployed_signal_registry_contract_address_

# AI Engine Configurations
LLM_API_KEY=your_openai_or_gemini_api_key
LLM_BASE_URL=your_llm_api_base_url
```

### 2. Frontend Setup
Create a `.env.local` file under the `signal-hub-web/` directory:
```env
NEXT_PUBLIC_API_URL=http://localhost:8000/api/v1
NEXTAUTH_SECRET=your_nextauth_session_secret
GOOGLE_CLIENT_ID=your_google_oauth_client_id
GOOGLE_CLIENT_SECRET=your_google_oauth_client_secret
```

---

## 🏃‍♂️ Running Guide

### 1. Compile & Deploy Smart Contract
Compile and deploy `SignalRegistry.sol` to **Mantle Sepolia Testnet** using Hardhat or Foundry. Copy the deployment address and update your environment variables.

### 2. Launch the Blockchain Batcher Daemon
```bash
cd blockchain-worker
python batcher.py
```
*This activates the Kafka real-time stream subscription consumer along with the asynchronous dual-track periodic anchoring monitor.*

### 3. Configure Telegram Bot Webhook
Fire up your tunneling tool (e.g., `ngrok http 8000`), grab the latest external HTTPS forwarding address, and trigger the webhook registration via your browser:
```text
[https://api.telegram.org/bot](https://api.telegram.org/bot)<YOUR_BOT_TOKEN>/setWebhook?url=<YOUR_NGROK_URL>/api/v1/bot/webhook
```

### 4. Start the FastAPI Gateway
```bash
cd backend-api
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### 5. Launch the Next.js Frontend Matrix
```bash
cd signal-hub-web
npm install
npm run dev
```

---

## 📜 Smart Contract Interface Specification

### `publishAlertBatch`
```solidity
struct AlertItem {
    string txHash;
    address target;
    string eventType;
}
function publishAlertBatch(AlertItem[] calldata _alerts) external onlyOwner;
```
* **Purpose:** Receives, filters, and records stream alert entries forwarded by the Batcher daemon, establishing decentralized cryptographic data integrity storage.

### `publishTopAddresses`
```solidity
struct TopAddressItem {
    address targetAddress;
    uint8 aiScore;
    string reason;
}
function publishTopAddresses(TopAddressItem[] calldata _topAddresses) external onlyOwner;
```
* **Purpose:** Anchors daily LLM evaluations—such as win-rates, PnL ratios, and attribution tags—solidifying the data sovereignty of the AI Agent network.