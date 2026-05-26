# AML Streaming - Real-Time Anti-Money Laundering Monitoring System

A real-time AML monitoring and reporting system built on Lambda architecture, compliant with FATF international standards.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Kafka Topics                                  │
│  txn.bank / txn.card / txn.forex / entity.customer              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                  ┌──────┴──────┐
                  ▼             ▼
┌───────────────────────┐ ┌───────────────────────────────────────┐
│    Real-Time Layer    │ │         Batch Layer                   │
│  Spark Structured     │ │  Spark on Hive                       │
│  Streaming            │ │  - Feature Engineering (User Profile) │
│  - Layer 1: Normalize │ │  - KYC/CDD Batch Assessment          │
│  - Layer 2: Enrich    │ │  - Summary Report Generation         │
│  - Layer 3: Rules     │ │  - Alert Reconciliation              │
│  - Layer 4: Aggregate │ │                                      │
└───────────┬───────────┘ └──────────────────┬────────────────────┘
            │                                │
            ▼                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                       Serving Layer                              │
│  ClickHouse: Alerts & Analytics  │  HBase: Customer Entities    │
│  Redis: Rules Cache & Bloom Filter                              │
└──────────────────────────┬──────────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  Spring Boot REST API: Alert Workflow / Rule Management         │
└─────────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Scala 2.12 / Java 8 |
| Stream Processing | Apache Spark 3.5.1 Structured Streaming |
| Message Queue | Apache Kafka 3.7.0 |
| Columnar Store | ClickHouse 24.3 |
| Wide-Column Store | Apache HBase 2.5.8 |
| Data Warehouse | Apache Hive 3.1.3 |
| Cache | Redis 7 |
| REST API | Spring Boot 2.7.18 |
| Build Tool | Maven |

## Project Structure

```
aml-streaming/
├── aml-common/           # Shared models, config, serialization
├── aml-streaming/        # Real-time pipeline (4 layers)
│   ├── layer1/           # Data normalization (multi-source → canonical)
│   ├── layer2/           # Feature enrichment & window aggregation
│   ├── layer3/           # Rule engine (YAML DSL) & sanctions screening
│   └── layer4/           # Alert aggregation & deduplication
├── aml-batch/            # Batch processing
│   ├── feature/          # User profile & risk scoring
│   ├── report/           # FATF XML report generation
│   └── reconciliation/   # Real-time/offline consistency check
├── aml-service/          # Spring Boot REST API
├── docker/               # Docker Compose & init SQL
├── scripts/              # Seed data & utilities
└── docs/                 # Design docs & plans
```

## Quick Start

### 1. Start Infrastructure

```bash
cd docker
docker-compose up -d
```

This starts: Zookeeper, Kafka, Redis, ClickHouse, HBase, Hive.

### 2. Build

```bash
mvn clean package -DskipTests
```

### 3. Seed Test Data

```bash
chmod +x scripts/seed-data.sh
./scripts/seed-data.sh
```

### 4. Run Streaming Pipeline

```bash
# Layer 1: Normalize transactions
spark-submit --class com.aml.streaming.layer1.NormalizationJob aml-streaming/target/aml-streaming-1.0-SNAPSHOT.jar

# Layer 2: Feature enrichment
spark-submit --class com.aml.streaming.layer2.FeatureEnrichmentJob aml-streaming/target/aml-streaming-1.0-SNAPSHOT.jar

# Layer 3: Rule engine
spark-submit --class com.aml.streaming.layer3.RuleEngineJob aml-streaming/target/aml-streaming-1.0-SNAPSHOT.jar

# Layer 4: Alert aggregation
spark-submit --class com.aml.streaming.layer4.AlertAggregationJob aml-streaming/target/aml-streaming-1.0-SNAPSHOT.jar
```

### 5. Run REST API

```bash
java -jar aml-service/target/aml-service-1.0-SNAPSHOT.jar
```

API available at `http://localhost:8080`.

## REST API Endpoints

### Alert Workflow

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/alerts/new` | Get new alerts |
| GET | `/api/alerts/customer/{id}` | Get alerts by customer |
| POST | `/api/alerts/{id}/review` | Review alert (confirm/escalate/close) |
| POST | `/api/alerts/{id}/report` | Generate report for alert |

### Rule Management

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/rules/versions` | List all rule versions |
| GET | `/api/rules/effective` | Get effective rules for timestamp |
| POST | `/api/rules/versions` | Create new rule version |
| POST | `/api/rules/versions/{id}/deprecate` | Deprecate rule version |

## Rule Engine

Rules are defined in YAML with a lightweight DSL:

```yaml
rules:
  - id: SAR-001
    name: "Structuring Detection"
    type: SAR
    severity: HIGH
    window: 24h
    conditions:
      - field: txn_count_24h
        operator: GTE
        value: 5
      - field: max_single_amount
        operator: LT
        value: 10000
      - field: total_amount_24h
        operator: GTE
        value: 40000
    actions:
      - type: ALERT
        template: "Potential structuring: {txn_count_24h} transactions totaling ${total_amount_24h}"
      - type: ESCALATE
        priority: HIGH
```

Supported operators: `GTE`, `GT`, `LTE`, `LT`, `EQ`, `NEQ`, `IN`, `NOT_IN`.

## Pre-defined Detection Rules

| Rule ID | Type | Description |
|---------|------|-------------|
| CTR-001 | CTR | Large cash transaction (>= $10,000 via ATM/Branch) |
| SAR-001 | SAR | Structuring detection (5+ txns, each < $10k, total >= $40k) |
| SAR-002 | SAR | Anomalous frequency (5x historical average) |
| SAR-003 | SAR | High-risk country transfer |
| SAR-004 | SAR | Rapid in-out (deposit then withdraw, retention < 10%) |
| SAN-001 | Sanctions | Sanctions list match (Bloom Filter + Jaro-Winkler fuzzy) |

## Configuration

All configs support environment variable override via `${?ENV_VAR}`:

| Config | Env Var | Default |
|--------|---------|---------|
| Kafka bootstrap | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| ClickHouse JDBC | `CLICKHOUSE_JDBC_URL` | `jdbc:clickhouse://localhost:8123/aml` |
| ClickHouse password | `CLICKHOUSE_PASSWORD` | (empty) |
| HBase ZooKeeper | `HBASE_ZOOKEEPER_QUORUM` | `localhost` |
| Redis host | `REDIS_HOST` | `localhost` |
| Hive metastore | `HIVE_METASTORE_URIS` | `thrift://localhost:9083` |

## License

Internal project. Not for distribution.
