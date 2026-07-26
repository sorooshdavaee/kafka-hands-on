#!/bin/bash
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka-1:19092,kafka-2:19092,kafka-3:19092}"
BIN=/opt/kafka/bin

echo "Waiting for cluster..."
sleep 5

create_topic() {
  local name=$1
  shift
  if $BIN/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list | grep -qx "$name"; then
    echo "Topic '$name' already exists"
  else
    echo "Creating topic '$name'..."
    $BIN/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic "$name" "$@"
  fi
  $BIN/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$name"
}

# Step 1: orders — 6 partitions, RF=3 (ISR / Leader / Replicas)
create_topic orders --partitions 6 --replication-factor 3

# Step 5: payment-results (transactional read-process-write target)
create_topic payment-results --partitions 6 --replication-factor 3

# Step 7: compacted topic — latest status per customer (key = CustomerId)
create_topic customer-latest-status \
  --partitions 6 \
  --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=10000

echo "All topics ready."
