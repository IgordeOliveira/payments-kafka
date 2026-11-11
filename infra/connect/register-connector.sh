#!/bin/sh
# Registers (or updates) the Debezium outbox connector. Idempotent: it PUTs the
# config to /connectors/<name>/config, which creates the connector the first
# time and updates it in place afterwards, so there is never a duplicate.
# Runs inside the curlimages/curl image (POSIX sh), launched by `connector-init`.
set -eu

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
CONNECTOR_NAME="${CONNECTOR_NAME:-payments-outbox}"
CONFIG_FILE="${CONFIG_FILE:-/infra/connect/outbox-connector.json}"

echo "Waiting for Kafka Connect at ${CONNECT_URL}"
i=0
until curl -fsS -o /dev/null "${CONNECT_URL}/connectors"; do
  i=$((i + 1))
  if [ "$i" -ge 60 ]; then echo "Kafka Connect not reachable" >&2; exit 1; fi
  sleep 2
done

echo "Registering connector ${CONNECTOR_NAME}"
curl -fsS -X PUT -H 'Content-Type: application/json' \
  --data @"${CONFIG_FILE}" \
  "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config"
echo

echo "Waiting for connector ${CONNECTOR_NAME} and its task to be RUNNING"
i=0
while :; do
  status="$(curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" || true)"
  # RUNNING must appear twice: once for the connector, once for task 0.
  running="$(printf '%s' "$status" | grep -o '"state":"RUNNING"' | wc -l)"
  if [ "$running" -ge 2 ]; then
    echo "$status"
    echo "Connector ${CONNECTOR_NAME} is RUNNING"
    exit 0
  fi
  if printf '%s' "$status" | grep -q '"state":"FAILED"'; then
    echo "$status" >&2
    echo "Connector ${CONNECTOR_NAME} FAILED" >&2
    exit 1
  fi
  i=$((i + 1))
  if [ "$i" -ge 30 ]; then echo "$status" >&2; echo "Timed out waiting for RUNNING" >&2; exit 1; fi
  sleep 2
done
