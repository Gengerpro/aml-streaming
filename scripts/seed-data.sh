#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SAMPLE_FILE="${SCRIPT_DIR}/sample-transactions.json"
BOOTSTRAP_SERVER="localhost:9092"

echo "=== Creating Kafka topics ==="

TOPICS=(
  "txn.bank:3"
  "txn.card:3"
  "txn.forex:3"
  "txn.normalized:6"
  "txn.enriched:6"
  "alert.raw:3"
  "txn.dlq:3"
)

for topic_spec in "${TOPICS[@]}"; do
  topic="${topic_spec%%:*}"
  partitions="${topic_spec##*:}"
  echo "Creating topic: ${topic} (partitions=${partitions})"
  kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 1
done

echo ""
echo "=== Producing sample transactions ==="

# Map txnType to topic
get_topic() {
  case "$1" in
    TRANSFER) echo "txn.bank" ;;
    CARD)     echo "txn.card" ;;
    FOREX)    echo "txn.forex" ;;
    *)        echo "txn.dlq" ;;
  esac
}

TXN_COUNT=$(jq 'length' "${SAMPLE_FILE}")

for i in $(seq 0 $((TXN_COUNT - 1))); do
  txn_json=$(jq ".[$i]" "${SAMPLE_FILE}")
  txn_type=$(echo "${txn_json}" | jq -r '.txnType')
  txn_id=$(echo "${txn_json}" | jq -r '.txnId')
  topic=$(get_topic "${txn_type}")

  echo "Producing ${txn_id} (${txn_type}) -> ${topic}"
  echo "${txn_json}" | kafka-console-producer \
    --broker-list "${BOOTSTRAP_SERVER}" \
    --topic "${topic}"
done

echo ""
echo "=== Seed complete: ${TXN_COUNT} transactions produced ==="
