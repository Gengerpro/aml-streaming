CREATE DATABASE IF NOT EXISTS aml;

CREATE TABLE IF NOT EXISTS aml.txn_normalized
(
    txn_id          STRING,
    txn_type        STRING,
    timestamp       BIGINT,
    amount          DECIMAL(18,2),
    currency        STRING,
    amount_usd      DECIMAL(18,2),
    direction       STRING,
    customer_id     STRING,
    counterparty_id STRING,
    channel         STRING,
    country_src     STRING,
    country_dst     STRING,
    metadata        MAP<STRING, STRING>,
    raw_payload     STRING
)
PARTITIONED BY (dt STRING, txn_type STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS aml.txn_enriched
(
    txn_id            STRING,
    txn_type          STRING,
    timestamp         BIGINT,
    amount            DECIMAL(18,2),
    currency          STRING,
    amount_usd        DECIMAL(18,2),
    direction         STRING,
    customer_id       STRING,
    counterparty_id   STRING,
    channel           STRING,
    country_src       STRING,
    country_dst       STRING,
    metadata          MAP<STRING, STRING>,
    raw_payload       STRING,
    txn_count_1h      BIGINT,
    total_amount_1h   DECIMAL(18,2),
    txn_count_24h     BIGINT,
    total_amount_24h  DECIMAL(18,2)
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS aml.alert_history
(
    alert_id     STRING,
    txn_id       STRING,
    customer_id  STRING,
    alert_type   STRING,
    severity     STRING,
    rule_id      STRING,
    rule_desc    STRING,
    score        FLOAT,
    status       STRING,
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP,
    reviewer_id  STRING,
    notes        STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS aml.entity_snapshot
(
    customer_id    STRING,
    name           STRING,
    id_type        STRING,
    id_number      STRING,
    nationality    STRING,
    occupation     STRING,
    income_source  STRING,
    address        STRING,
    kyc_level      STRING,
    customer_type  STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS aml.rule_versions
(
    version_id      STRING,
    effective_from  TIMESTAMP,
    rules_json      STRING,
    created_by      STRING,
    created_at      TIMESTAMP,
    status          STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET;
