CREATE DATABASE IF NOT EXISTS aml;

CREATE TABLE IF NOT EXISTS aml.alert_queue
(
    alert_id     String,
    txn_id       String,
    customer_id  String,
    alert_type   Enum8('STRUCTURING' = 1, 'LAYERING' = 2, 'SMURFING' = 3, 'RAPID_MOVEMENT' = 4, 'UNUSUAL_PATTERN' = 5, 'HIGH_RISK_COUNTRY' = 6, 'OTHER' = 7),
    severity     Enum8('LOW' = 1, 'MEDIUM' = 2, 'HIGH' = 3, 'CRITICAL' = 4),
    rule_id      String,
    rule_desc    String,
    score        Float32,
    status       Enum8('OPEN' = 1, 'INVESTIGATING' = 2, 'CLOSED_FALSE_POSITIVE' = 3, 'CLOSED_CONFIRMED' = 4, 'ESCALATED' = 5),
    created_at   DateTime64(3),
    updated_at   DateTime64(3),
    reviewer_id  Nullable(String),
    notes        Nullable(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (severity, customer_id, created_at)
TTL created_at + INTERVAL 3 YEAR;

CREATE TABLE IF NOT EXISTS aml.daily_summary
(
    report_date       Date,
    total_transactions UInt64,
    total_amount      Decimal18(2),
    unique_customers  UInt64,
    ctr_count         UInt32,
    sar_count         UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(report_date)
ORDER BY report_date;

CREATE TABLE IF NOT EXISTS aml.reconciliation_log
(
    report_date         Date,
    batch_alerts        UInt64,
    realtime_alerts     UInt64,
    missing_in_realtime UInt64,
    extra_in_realtime   UInt64,
    diff_rate           Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(report_date)
ORDER BY report_date;
