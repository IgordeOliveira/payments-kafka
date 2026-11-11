#!/usr/bin/env bash
# Experiment 3: Kafka goes down while the API keeps accepting payments.
# See experiments/03-kafka-outage.md.
source /workspace/experiments/lib/common.sh

SEED="${SEED:-303}"
PAYMENTS="${PAYMENTS:-600}"
RATE="${RATE:-25}"
OUTAGE_SECONDS="${OUTAGE_SECONDS:-20}"
STOP_AFTER_ROWS="${STOP_AFTER_ROWS:-100}"

require_stack
reset_pipeline

T0=$(psql_payments 'SELECT now()')
outbox_rows() { psql_payments "SELECT count(*) FROM outbox WHERE created_at >= '$T0'"; }
api_healthy() { curl -fsS -m 3 http://payment-api:8081/actuator/health > /dev/null 2>&1; }
kafka_healthy() { [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = healthy ]; }

step "Normal-mode run (simulator -> payment API -> outbox -> Debezium): seed $SEED, $PAYMENTS payments at $RATE steps/s"
sim_start SIMULATOR_MODE=normal SIMULATOR_SEED="$SEED" SIMULATOR_MAX_PAYMENTS="$PAYMENTS" SIMULATOR_RATE_PER_SECOND="$RATE"

enough_rows() { [ "$(outbox_rows)" -ge "$STOP_AFTER_ROWS" ]; }
wait_until 120 "$STOP_AFTER_ROWS outbox rows" enough_rows || die "the API did not write outbox rows"

step "Stopping the Kafka broker for ${OUTAGE_SECONDS}s while the simulator keeps calling the API"
docker stop kafka > /dev/null
rows_at_stop=$(outbox_rows)
stats_at_stop=$(sim_stats)
log "kafka stopped; outbox rows so far: $rows_at_stop"

probes=0; failed_probes=0
deadline=$((SECONDS + OUTAGE_SECONDS))
while [ "$SECONDS" -lt "$deadline" ]; do
  probes=$((probes + 1))
  api_healthy || failed_probes=$((failed_probes + 1))
  log "broker down: outbox rows $(outbox_rows), simulator $(sim_stats | jq -c '{events_sent_total, api}')"
  sleep 2
done
rows_at_start=$(outbox_rows)
stats_at_start=$(sim_stats)

step "Starting the Kafka broker again"
docker start kafka > /dev/null
wait_until 180 "kafka healthy" kafka_healthy || die "kafka did not become healthy"
log "kafka is healthy; connector: $(curl -fsS http://kafka-connect:8083/connectors/payments-outbox/status | jq -c '[.connector.state, .tasks[].state]')"

sim_wait 300 "$WORK_DIR/stats.json"
sim_stop

step "Waiting until Debezium published every outbox row of the run"
outbox_ids() { psql_payments "SELECT id FROM outbox WHERE created_at >= '$T0'" | sort -u; }
caught_up() {
  local rows topic
  rows=$(outbox_rows); topic=$(distinct_event_ids "$MAIN_TOPIC" | wc -l)
  log "outbox rows $rows, distinct events on topic $topic, connector $(curl -fsS http://kafka-connect:8083/connectors/payments-outbox/status | jq -c '[.connector.state, .tasks[].state]')"
  [ "$rows" = "$topic" ]
}
caught_up_rc=0
wait_until 240 "Debezium caught up" caught_up || caught_up_rc=$?
drain_rc=0
wait_drained 300 $CONSUMER_GROUPS || drain_rc=$?

if [ "${SABOTAGE:-0}" = 1 ]; then
  step "SABOTAGE: removing one payment from reconciliation, as if its events had been lost"
  psql_recon "DELETE FROM payments WHERE payment_id = (SELECT payment_id FROM payments ORDER BY payment_id LIMIT 1)" > /dev/null
fi

outbox_ids > "$WORK_DIR/outbox-ids.txt"
distinct_event_ids "$MAIN_TOPIC" > "$WORK_DIR/topic-ids.txt"
psql_payments "SELECT id || ' ' || status || ' ' || amount_cents FROM payments WHERE created_at >= '$T0' ORDER BY 1" > "$WORK_DIR/api-payments.txt"
psql_recon "SELECT payment_id || ' ' || status || ' ' || amount_cents FROM payments ORDER BY 1" > "$WORK_DIR/recon-payments.txt"

rows=$(wc -l < "$WORK_DIR/outbox-ids.txt")
processed=$(processed_events)
check_rc=0; recon_check "$WORK_DIR/check.json" || check_rc=$?
during=$((rows_at_start - rows_at_stop))
sent_during=$(( $(echo "$stats_at_start" | jq .events_sent_total) - $(echo "$stats_at_stop" | jq .events_sent_total) ))

measure "outbox rows when the broker stopped" "$rows_at_stop"
measure "outbox rows when the broker started" "$rows_at_start"
measure "outbox rows written during the outage" "$during"
measure "API 2xx answers during the outage (simulator events_sent delta)" "$sent_during"
measure "API health probes during the outage (failed / total)" "$failed_probes / $probes"
measure "simulator events_sent_total / api counters" "$(jq .events_sent_total "$WORK_DIR/stats.json") / $(jq -c .api "$WORK_DIR/stats.json")"
measure "outbox rows of the run" "$rows"
measure "records on $MAIN_TOPIC (incl. at-least-once republishes)" "$(topic_size "$MAIN_TOPIC")"
measure "distinct event_ids on $MAIN_TOPIC" "$(wc -l < "$WORK_DIR/topic-ids.txt")"
measure "payments in API database / reconciliation" "$(wc -l < "$WORK_DIR/api-payments.txt") / $(wc -l < "$WORK_DIR/recon-payments.txt")"
measure "reconciliation processed_events" "$processed"
measure "check report" "$(jq -c '{ok, merchants, ledger_entries, processed_events, mismatches: (.mismatches | length)}' "$WORK_DIR/check.json")"
measure "DLQ records (all groups)" "$(dlq_summary | tr '\n' ' ')"

assert_gt "outbox rows accumulated while the broker was down" 0 "$during"
assert_gt "the API accepted requests during the outage" 0 "$sent_during"
assert_eq "API health probes failed during the outage" 0 "$failed_probes"
assert_eq "the simulator got no API error or retry" '{"retries":0,"conflicts":0,"errors":0,"skipped":0}' "$(jq -c .api "$WORK_DIR/stats.json")"
assert_eq "every API call of the run wrote one outbox row (events_sent_total = outbox rows)" "$(jq .events_sent_total "$WORK_DIR/stats.json")" "$rows"
assert_eq "Debezium caught up after the outage" 0 "$caught_up_rc"
assert_files_equal "distinct event_ids on the topic = outbox ids" "$WORK_DIR/outbox-ids.txt" "$WORK_DIR/topic-ids.txt"
assert_eq "consumer groups drained after the outage" 0 "$drain_rc"
assert_eq "every distinct event applied once (processed_events = outbox rows)" "$rows" "$processed"
assert_files_equal "reconciliation holds every API payment with the same status and amount" "$WORK_DIR/api-payments.txt" "$WORK_DIR/recon-payments.txt"
assert_eq "reconciliation check passes (exit code)" 0 "$check_rc"
assert_eq "nothing dead-lettered" 0 "$(dlq_count "")"

finish kafka-outage
