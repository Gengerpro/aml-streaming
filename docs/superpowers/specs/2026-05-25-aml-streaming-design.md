# Real-Time AML Monitoring and Reporting System - Design Spec

## Overview

A real-time anti-money laundering (AML) monitoring and reporting system built on a Lambda architecture, designed to comply with FATF international standards. The system monitors bank transfers, card transactions, and forex trades at 100M+ transactions/day scale, with a target of 30-minute processing for high-risk alerts and T+1 for summary reports.

## Requirements Summary

| Dimension | Choice |
|-----------|--------|
| Regulatory Framework | FATF International Standards |
| Tech Stack | Java/Scala + Kafka + Spark Structured Streaming + Hive + HBase + ClickHouse |
| Transaction Types | Bank transfers/remittances, Card transactions, Forex transactions |
| Scale | 100M+ transactions/day |
| Core Capabilities | Rule engine, Sanctions screening, KYC/CDD, User feature engineering |
| Reporting | CTR, SAR/STR, Periodic summary, Regulatory API integration |
| Latency | High-risk: 6h (target 30min), Summary: T+1 |
| Data Source | Kafka Topics |
| Alert Workflow | Full lifecycle: Alert → Review → Investigation → Reporting |
| Graph Analysis | Deferred to future phase |

## Architecture: Lambda Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Data Ingestion Layer                         │
│  Kafka Topics: txn.bank / txn.card / txn.forex / entity.customer   │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                    ┌──────┴──────┐
                    ▼             ▼
┌─────────────────────────┐ ┌─────────────────────────────────────────┐
│    Real-Time Layer      │ │         Batch Layer                     │
│  Spark Structured       │ │  Spark on Hive                         │
│  Streaming              │ │  - Feature Engineering (User Profile)  │
│  - Rule Engine          │ │  - KYC/CDD Batch Assessment            │
│  - Sanctions Screening  │ │  - Summary Report Generation           │
│  - CTR Auto-Generation  │ │  - Model Training/Update               │
│  - SAR Pattern Detection│ │  - Alert Reconciliation                │
└────────────┬────────────┘ └────────────────┬────────────────────────┘
             │                               │
             ▼                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Serving Layer                                │
│  ClickHouse: Alert queries / Analytics / Reports                    │
│  HBase: Customer entity / Transaction profile / KYC status          │
│  Redis: Rule cache / Sanctions Bloom Filter / Session state         │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Application Layer                            │
│  Alert workflow engine / Case management / Report generation        │
│  Regulatory API integration                                         │
└─────────────────────────────────────────────────────────────────────┘
```

## Real-Time Processing Pipeline (4 Layers + Bypass)

### Layer 1: Data Normalization

- Multi-source transaction data unified into Canonical Transaction Schema
- Field mapping, encoding conversion, currency standardization
- Data quality validation (invalid records → Dead Letter Queue)
- Partitioned by `txn_id` for uniform distribution
- Output: Kafka Topic `txn.normalized`

### Layer 2: Feature Enrichment & Window Aggregation

- Load customer profiles from HBase/Redis (risk level, KYC status, historical stats)
- Sliding window aggregation: 1h/24h/7d transaction frequency, cumulative amount, counterparty count
- Stateful Streaming: partitioned by `customer_id`
- Output: Kafka Topic `txn.enriched`

### Layer 3: Rule Engine & Sanctions Screening

- CTR detection: single transaction above threshold → auto-generate CTR report
- SAR rule matching: structuring, frequent transactions, anomalous patterns
- Sanctions screening: Bloom Filter fast filter + precise fuzzy matching
- Rule hot-reload: dynamic rule updates from DB/config center
- Output: Kafka Topics `alert.raw` / `ctr.event` / `sanction.hit`

### Layer 4: Traceability & Alert Aggregation

- Medium/high-risk alerts trigger fund chain traceability (last N hops)
- Correlation analysis: aggregate multiple alerts for same customer/counterparty
- Alert deduplication & priority sorting
- Write to alert workflow engine (ClickHouse + business system)

### Bypass Lanes (Anti-Skew)

```
                    ┌───────────────────────────────┐
                    │  Layer 1: Data Normalization   │
                    │  (partitioned by txn_id)       │
                    └───────────────┬───────────────┘
                          ┌─────────┴─────────┐
                          ▼                   ▼
              ┌───────────────────┐ ┌───────────────────┐
              │  Main Lane        │ │  Bypass Lane       │
              │  Normal customers │ │  Hot-key customers │
              └─────────┬─────────┘ └─────────┬─────────┘
                        │                     │
                        ▼                     ▼
          ┌─────────────────────┐ ┌─────────────────────┐
          │  Layer 2a: Sync     │ │  Layer 2b: Async     │
          │  HBase/Redis lookup │ │  Pre-computed feat.  │
          │  Window aggregation │ │  Independent job     │
          └─────────┬───────────┘ └─────────┬───────────┘
                    └──────────┬────────────┘
                               ▼
                  ┌───────────────────────────┐
                  │  Layer 3: Rule Engine      │
                  │  Stateless, horizontal     │
                  └─────────────┬─────────────┘
                       ┌────────┴────────┐
                       ▼                 ▼
              ┌──────────────┐ ┌──────────────┐
              │  Low risk    │ │  Med/High    │
              │  Auto-close  │ │  Trace       │
              └──────────────┘ └──────┬───────┘
                                      ▼
                        ┌───────────────────────┐
                        │  Layer 4: Aggregation  │
                        └───────────────────────┘
```

| Scenario | Main Lane | Bypass Lane |
|----------|-----------|-------------|
| High-frequency customer (>1000 txn/day) | Skip window calc, read pre-computed from Redis | Independent Spark Streaming Job updates Redis |
| Large transaction (above CTR threshold) | Skip to Layer 3, skip feature loading | — |
| Sanctions hit | Layer 3 direct red flag, priority processing | — |
| Batch deposits (same counterparty >N txns) | Aggregate by batch_id, process once | — |

Split decision logic is tagged at Layer 1 output; subsequent layers route by tag.

## Data Model

### Canonical Transaction Model

| Field | Type | Description |
|-------|------|-------------|
| txn_id | STRING | Global unique ID across all business lines |
| txn_type | ENUM | TRANSFER / CARD / FOREX |
| timestamp | BIGINT | Millisecond UTC |
| amount | DECIMAL | Original amount |
| currency | STRING | ISO 4217 |
| amount_usd | DECIMAL | Standardized to USD equivalent |
| direction | ENUM | INBOUND / OUTBOUND |
| customer_id | STRING | Initiator |
| counterparty_id | STRING | Receiver |
| channel | STRING | ATM / ONLINE / BRANCH / SWIFT |
| country_src | STRING | Origin country ISO 3166 |
| country_dst | STRING | Destination country ISO 3166 |
| metadata | MAP | Business-line specific fields |
| raw_payload | STRING | Original message for audit |

### Storage Layer Responsibilities

| Storage | Purpose | Data Characteristics | Retention |
|---------|---------|---------------------|-----------|
| **Kafka** | Real-time pipeline | Message queue, no persistence | 7 days |
| **Hive** | Raw data archive + batch processing | Partitioned (by day), Parquet | Permanent |
| **HBase** | Entity data + real-time profiles | Customer entity, KYC status, risk rating | Permanent |
| **ClickHouse** | Alerts + analytics queries | Columnar, OLAP queries | 3 years |
| **Redis** | Hot cache | Rule cache, sanctions Bloom Filter, window metrics | TTL-based |

### Hive Partitioning Strategy

```
/warehouse/aml/
├── txn_normalized/          # Layer 1 output archive
│   └── dt=2026-05-25/
│       └── txn_type=BANK/
├── txn_enriched/            # Layer 2 output archive
│   └── dt=2026-05-25/
├── alert_history/           # Alert history archive
│   └── dt=2026-05-25/
├── entity_snapshot/         # Daily entity snapshot
│   └── dt=2026-05-25/
└── rule_versions/           # Rule version snapshots
    └── dt=2026-05-25/
```

### ClickHouse Alert Table

```sql
CREATE TABLE aml.alert_queue (
    alert_id        String,
    txn_id          String,
    customer_id     String,
    alert_type      Enum8('CTR'=1, 'SAR'=2, 'SANCTION'=3, 'RULE'=4),
    severity        Enum8('LOW'=1, 'MEDIUM'=2, 'HIGH'=3, 'CRITICAL'=4),
    rule_id         String,
    rule_desc       String,
    score           Float32,
    status          Enum8('NEW'=1, 'REVIEWING'=2, 'ESCALATED'=3, 'CLOSED'=4, 'REPORTED'=5),
    created_at      DateTime64(3),
    updated_at      DateTime64(3),
    reviewer_id     Nullable(String),
    notes           Nullable(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (severity, customer_id, created_at)
TTL created_at + INTERVAL 3 YEAR;
```

### HBase Customer Entity Table

```
RowKey: customer_id (hash prefix + customer_id for hotspot avoidance)

Column Families:
├── cf:profile        # Basic profile
│   ├── name, id_type, id_number, nationality
│   ├── occupation, income_source, address
│   └── kyc_level, kyc_expiry
├── cf:risk           # Risk rating
│   ├── risk_level (LOW/MEDIUM/HIGH/PROHIBITED)
│   ├── risk_score, risk_factors_json
│   └── last_review_date, next_review_date
├── cf:txn_stats      # Transaction stats (updated by Layer 2)
│   ├── txn_count_7d, txn_count_30d
│   ├── total_amount_7d, total_amount_30d
│   └── avg_amount, max_amount, unique_counterparties_30d
└── cf:alert          # Alert history
    ├── alert_count_total, alert_count_90d
    ├── last_alert_date, last_alert_type
    └── open_alert_count
```

## Rule Engine

### Architecture

- Rule DSL for defining detection rules (YAML-based)
- Configuration-driven lightweight rule execution engine (no external framework like Drools)
  - YAML rule definitions parsed into in-memory condition trees
  - Simple condition evaluator: field comparisons, threshold checks, set membership
  - Composite rules via AND/OR/NOT logical operators
  - Window-based conditions evaluated against pre-computed features from Layer 2
- Rule version management with effective_from timestamps
- Hot-reload: DB → Redis → Streaming (atomic swap)
- Rules matched by transaction timestamp, not processing timestamp

### Rule Hot-Reload & Consistency

**Rule Version Timeline:**
- Each rule version has `effective_from` timestamp
- Lookup: `SELECT * FROM rule_versions WHERE effective_from <= txn.timestamp ORDER BY effective_from DESC LIMIT 1`
- Both real-time and offline pipelines match rules by transaction time

**Hot-Reload Flow:**
1. Rule admin creates/modifies rule with future `effective_from`
2. CDC/polling detects change in DB
3. Redis cache updated: rule snapshot + version index (sorted set by time)
4. Streaming Job refreshes or receives broadcast notification
5. New rule version loaded into memory

**Real-time/Offline Consistency:**
1. Unified rule storage: Hive `aml.rule_versions` table
2. Snapshot mechanism: each rule change generates complete snapshot
3. Transaction-time matching: real-time reads Redis sorted set, offline reads Hive
4. Reconciliation: T+1 offline job compares real-time alert results, diff rate target < 0.1%

### Pre-defined Rules

| Rule ID | Type | Description | Trigger Condition |
|---------|------|-------------|-------------------|
| CTR-001 | CTR | Large cash transaction | Single txn ≥ $10,000 AND channel IN (ATM, BRANCH) |
| SAR-001 | SAR | Structuring detection | 24h: ≥5 txns, each <$10,000, total ≥$40,000 |
| SAR-002 | SAR | Anomalous frequency | 1h: frequency ≥ 5× historical average |
| SAR-003 | SAR | High-risk country transfer | country_dst IN high-risk list AND amount ≥$5,000 |
| SAR-004 | SAR | Rapid in-out | 24h: deposit then withdraw, retention <10% |
| SAN-001 | Sanctions | Sanctions list match | Customer/counterparty matches OFAC/EU/UN (fuzzy ≥85%) |

### Rule DSL Example

```yaml
rule:
  id: SAR-001
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
    - field: channel
      operator: IN
      value: [ATM, BRANCH, ONLINE]
  actions:
    - type: ALERT
      template: "Potential structuring: {txn_count_24h} transactions totaling ${total_amount_24h}"
    - type: ESCALATE
      priority: HIGH
```

## KYC/CDD System

### Customer Due Diligence

- Basic information collection at account opening
- Identity document verification (ID number + validity)
- Address proof
- Occupation/income source
- Transaction purpose declaration

### Risk Rating

- Customer risk levels: LOW / MEDIUM / HIGH / PROHIBITED
- Rating factors:
  - Country risk score
  - Product risk score
  - Customer type (individual / corporate / PEP)
  - Transaction pattern score
- Composite score = weighted_sum(factor_scores)

### Ongoing Monitoring

- Periodic review: HIGH (quarterly), MEDIUM (annually), LOW (every 2 years)
- Triggered review: automatic when transaction patterns change significantly
- Review results update HBase customer profile

## Sanctions Screening

### Two-Phase Screening

**Phase 1: Bloom Filter Fast Filter**
- Build Bloom Filter from sanctions lists, resident in memory
- Hit → proceed to Phase 2
- Miss → safe pass (O(1))

**Phase 2: Precise Fuzzy Matching**
- Levenshtein distance ≤ 2
- Jaro-Winkler similarity ≥ 85%
- Alias/former name matching
- Hit → generate sanctions alert, priority CRITICAL

### List Sources

- OFAC SDN (US)
- EU Consolidated List
- UN Security Council
- Custom internal high-risk lists

### List Update Flow

- New list → rebuild Bloom Filter → atomic Redis swap
- Incremental update → merge into existing filter
- Update audit log

## Alert Workflow & Reporting

### Alert Lifecycle

```
NEW → REVIEWING → ESCALATED → REPORTED → CLOSED
  └──→ CLOSED (false positive)
```

### Workflow Engine

1. **Alert Reception & Assignment**: consume from Kafka/ClickHouse, auto-assign by severity + rule type, SLA timer starts
2. **Review Interface (Case Management)**: alert details (transaction info + triggered rule + customer profile), associated transaction timeline, actions (confirm suspicious / exclude false positive / escalate / add notes)
3. **Report Generation**:
   - CTR: auto-generated by real-time layer, no manual review needed
   - SAR/STR: triggered after manual confirmation
   - Periodic summary: T+1 offline job
4. **Regulatory API Integration**: report formatting (FATF XML/JSON standard), submission queue & retry mechanism, submission status tracking & receipt management

### SLA Requirements

| Alert Level | Review Deadline | Reporting Deadline | Auto-Escalation |
|-------------|-----------------|-------------------|-----------------|
| CRITICAL | 30 min | 2h | Auto-escalate to supervisor on timeout |
| HIGH | 2h | 6h | Auto-escalate to supervisor on timeout |
| MEDIUM | 8h | 24h | Reminder on timeout |
| LOW | 24h | T+1 | None |

### Report Types

| Report Type | Trigger | Pipeline | Output Format |
|-------------|---------|----------|---------------|
| CTR | Real-time auto | Spark Streaming → ClickHouse | FATF XML |
| SAR/STR | Manual confirmation | API → Report generation service | FATF XML |
| Periodic Summary | Scheduled (T+1) | Spark Batch → Hive → ClickHouse | PDF + XML |
| Sanctions Hit | Real-time auto | Streaming → Instant notification | JSON + Email |

## Project Structure

```
aml-streaming/
├── pom.xml                                    # Maven parent POM
├── aml-common/                                # Common module
│   └── src/main/scala/com/aml/common/
│       ├── model/                             # Data models
│       ├── config/                            # Configuration
│       ├── util/                              # Utilities
│       └── serialization/                     # Kafka SerDe
├── aml-streaming/                             # Real-time processing (Spark Structured Streaming)
│   └── src/main/scala/com/aml/streaming/
│       ├── layer1/                            # Layer 1: Data normalization
│       ├── layer2/                            # Layer 2: Feature enrichment & window aggregation
│       ├── layer3/                            # Layer 3: Rule engine & sanctions screening
│       └── layer4/                            # Layer 4: Traceability & alert aggregation
├── aml-batch/                                 # Batch processing (Spark on Hive)
│   └── src/main/scala/com/aml/batch/
│       ├── feature/                           # Feature engineering
│       ├── kyc/                               # KYC/CDD batch assessment
│       ├── report/                            # Report generation
│       └── reconciliation/                    # Real-time/offline reconciliation
├── aml-service/                               # Business service (Spring Boot)
│   └── src/main/java/com/aml/service/
│       ├── alert/                             # Alert workflow
│       ├── case/                              # Case management
│       ├── rule/                              # Rule management
│       ├── kyc/                               # KYC management
│       └── report/                            # Report & regulatory integration
├── aml-web/                                   # Frontend (admin dashboard)
├── docker/                                    # Docker configuration
├── docs/                                      # Documentation
└── scripts/                                   # Operations scripts
```

## Anti-Skew Strategies

- Layer 1: partition by `txn_id` for uniform distribution
- Layer 2: partition by `customer_id`, use salting for high-frequency customers
- Layer 3: stateless processing, horizontally scalable
- Layer 4: timeout and depth limits on traceability queries to prevent chain explosion
- Bypass lanes for hot-key scenarios (high-frequency customers, large transactions, sanctions hits, batch deposits)
