#!/bin/sh
# Creates the pipeline topics. Idempotent: safe to run any number of times.
# Runs inside the apache/kafka image (POSIX sh), launched by the `kafka-init` service.
set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
TOPICS_BIN=/opt/kafka/bin/kafka-topics.sh

# name:partitions (replication factor is always 1, single broker)
TOPICS="
pix.payments.v1:6
pix.fraud-alerts.v1:6
pix.payments.retry.v1:6
pix.payments.dlq.v1:1
"

for entry in $TOPICS; do
  name="${entry%%:*}"
  partitions="${entry##*:}"
  echo "Ensuring topic ${name} (${partitions} partitions)"
  "$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 1
done

echo "Topics:"
"$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --describe --topic 'pix\..*'
