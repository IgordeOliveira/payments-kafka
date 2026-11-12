#!/usr/bin/env bash
# Experiment 5: concurrent load on the payment API, then pipeline consistency.
# See experiments/05-load.md. Not part of `experiments all`.
source /workspace/experiments/lib/common.sh

K6_IMAGE="${K6_IMAGE:-grafana/k6:2.3.0}"
K6_NETWORK="${K6_NETWORK:-payments-kafka_default}"
RATE="${RATE:-200}"
RAMP="${RAMP:-30s}"
DURATION="${DURATION:-2m}"
MERCHANTS="${MERCHANTS:-50}"
CONFIRM_RATIO="${CONFIRM_RATIO:-0.8}"
REFUND_RATIO="${REFUND_RATIO:-0.1}"
CATCHUP_TIMEOUT="${CATCHUP_TIMEOUT:-600}"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-600}"
SABOTAGE="${SABOTAGE:-0}"
RUN_ID="$(date -u +%Y%m%d%H%M%S)"
PREFIX="load-${RUN_ID}-"
SABOTAGE_LOCK_SECONDS="${SABOTAGE_LOCK_SECONDS:-1}"
SABOTAGE_PAUSE_SECONDS="${SABOTAGE_PAUSE_SECONDS:-1}"

# Sabotage: database contention. A background psql loop takes an EXCLUSIVE
# lock on the outbox table for SABOTAGE_LOCK_SECONDS, releases it for
# SABOTAGE_PAUSE_SECONDS, and repeats while the flag file exists. EXCLUSIVE
# blocks INSERTs (every API write inserts an outbox row) but not reads, so
# Debezium keeps streaming. Blocked requests wait, latency rises past the k6
# threshold, and each transaction still commits whole or not at all.
LOCK_FLAG="$WORK_DIR/sabotage.on"
LOCK_PID=""
start_lock_loop() {
  touch "$LOCK_FLAG"
  (
    while [ -f "$LOCK_FLAG" ]; do
      psql -d payments -q -c "BEGIN; LOCK TABLE outbox IN EXCLUSIVE MODE; SELECT pg_sleep($SABOTAGE_LOCK_SECONDS); COMMIT;" > /dev/null 2>&1 || true
      sleep "$SABOTAGE_PAUSE_SECONDS"
    done
  ) &
  LOCK_PID=$!
}
# stop_lock_loop: the EXIT trap when sabotaging, so no lock outlives the run
# whatever the outcome. Keeps the script's exit status.
stop_lock_loop() {
  local rc=$?
  trap - EXIT
  rm -f "$LOCK_FLAG"
  if [ -n "$LOCK_PID" ]; then wait "$LOCK_PID" 2> /dev/null || true; fi
  log "sabotage stopped: no lock is held on outbox"
  exit "$rc"
}

require_stack
reset_pipeline

LIKE="'${PREFIX}%'"
outbox_rows() { psql_payments "SELECT count(*) FROM outbox WHERE merchant_id LIKE $LIKE"; }
outbox_ids() { psql_payments "SELECT id FROM outbox WHERE merchant_id LIKE $LIKE" | sort -u; }

if [ "$SABOTAGE" = 1 ]; then
  step "SABOTAGE: locking the outbox table for ${SABOTAGE_LOCK_SECONDS}s every $((SABOTAGE_LOCK_SECONDS + SABOTAGE_PAUSE_SECONDS))s while k6 runs"
  trap stop_lock_loop EXIT
  start_lock_loop
fi

step "Load: k6 ($K6_IMAGE), $RATE iterations/s for $DURATION (ramp $RAMP), $MERCHANTS merchants, run $RUN_ID"
k6_rc=0
docker run --rm -i --network "$K6_NETWORK" \
  -e API_URL=http://payment-api:8081 -e RUN_ID="$RUN_ID" -e RATE="$RATE" -e RAMP="$RAMP" \
  -e DURATION="$DURATION" -e MERCHANTS="$MERCHANTS" -e CONFIRM_RATIO="$CONFIRM_RATIO" -e REFUND_RATIO="$REFUND_RATIO" \
  "$K6_IMAGE" run --quiet - < "$WORKSPACE/experiments/k6/load.js" > "$WORK_DIR/k6-summary.json" || k6_rc=$?
T_END=$SECONDS
if [ "$SABOTAGE" = 1 ]; then rm -f "$LOCK_FLAG"; wait "$LOCK_PID" 2> /dev/null || true; LOCK_PID=""; log "sabotage lock loop stopped"; fi
log "k6 exited with $k6_rc (99 = a threshold was crossed)"
jq -e . "$WORK_DIR/k6-summary.json" > /dev/null 2>&1 || die "k6 did not write a JSON summary to stdout (exit $k6_rc)"

step "Waiting until Debezium published every outbox row of the run"
caught_up() {
  local rows topic
  rows=$(outbox_rows); topic=$(distinct_event_ids "$MAIN_TOPIC" | wc -l | tr -d ' ')
  log "outbox rows $rows, distinct events on topic $topic"
  [ "$rows" = "$topic" ]
}
caught_up_rc=0
wait_until "$CATCHUP_TIMEOUT" "Debezium caught up" caught_up || caught_up_rc=$?
catchup_seconds=$((SECONDS - T_END))
drain_rc=0
wait_drained "$DRAIN_TIMEOUT" $CONSUMER_GROUPS || drain_rc=$?
drain_seconds=$((SECONDS - T_END))

outbox_ids > "$WORK_DIR/outbox-ids.txt"
distinct_event_ids "$MAIN_TOPIC" > "$WORK_DIR/topic-ids.txt"
psql_payments "SELECT id || ' ' || status || ' ' || amount_cents FROM payments WHERE merchant_id LIKE $LIKE ORDER BY 1" > "$WORK_DIR/api-payments.txt"
psql_recon "SELECT payment_id || ' ' || status || ' ' || amount_cents FROM payments ORDER BY 1" > "$WORK_DIR/recon-payments.txt"

rows=$(wc -l < "$WORK_DIR/outbox-ids.txt" | tr -d ' ')
implied=$(psql_payments "SELECT coalesce(sum(CASE status WHEN 'CREATED' THEN 1 WHEN 'CONFIRMED' THEN 2 WHEN 'EXPIRED' THEN 2 WHEN 'REFUNDED' THEN 3 END), 0) FROM payments WHERE merchant_id LIKE $LIKE")
processed=$(processed_events)
check_rc=0; recon_check "$WORK_DIR/check.json" || check_rc=$?

# jq helpers over the k6 summary. Absent counters count as 0; a missing
# threshold prints "missing" (never equals true).
k6num() { jq -r "(($1) // 0) | tostring" "$WORK_DIR/k6-summary.json"; }
k6round() { jq -r "(($1) // 0) | . * 100 | round / 100 | tostring" "$WORK_DIR/k6-summary.json"; }
k6thr() { jq -r "(($1) | if . == null then \"missing\" else tostring end)" "$WORK_DIR/k6-summary.json"; }

created=$(k6num .metrics.transitions_created.values.count)
confirmed=$(k6num .metrics.transitions_confirmed.values.count)
expired=$(k6num .metrics.transitions_expired.values.count)
refunded=$(k6num .metrics.transitions_refunded.values.count)
k6_transitions=$((created + confirmed + expired + refunded))
reqs=$(k6num .metrics.http_reqs.values.count)
reqs_rate=$(k6round .metrics.http_reqs.values.rate)
err_rate=$(jq -r '(.metrics.http_req_failed.values.rate // 0) | . * 10000 | round / 10000 | tostring' "$WORK_DIR/k6-summary.json")
p95=$(k6round '.metrics.http_req_duration.values["p(95)"]')
p99=$(k6round '.metrics.http_req_duration.values["p(99)"]')
avg=$(k6round '.metrics.http_req_duration.values.avg')
thr_err=$(k6thr '.metrics.http_req_failed.thresholds["rate<0.01"].ok')
thr_p95=$(k6thr '.metrics.http_req_duration.thresholds["p(95)<500"].ok')

measure "sabotage (outbox lock contention)" "$([ "$SABOTAGE" = 1 ] && echo on || echo off)"
measure "k6 run id / merchant prefix" "$RUN_ID / $PREFIX"
measure "k6 exit code (99 = threshold crossed)" "$k6_rc"
measure "k6 HTTP requests / requests per second" "$reqs / $reqs_rate"
measure "k6 error rate (http_req_failed)" "$err_rate"
measure "k6 latency ms: avg / p95 / p99" "$avg / $p95 / $p99"
measure "k6 transitions: created / confirmed / expired / refunded" "$created / $confirmed / $expired / $refunded"
measure "outbox rows of the run" "$rows"
measure "events implied by the API payment states" "$implied"
measure "records on $MAIN_TOPIC (incl. at-least-once republishes)" "$(topic_size "$MAIN_TOPIC")"
measure "distinct event_ids on $MAIN_TOPIC" "$(wc -l < "$WORK_DIR/topic-ids.txt" | tr -d ' ')"
measure "payments in API database / reconciliation" "$(wc -l < "$WORK_DIR/api-payments.txt" | tr -d ' ') / $(wc -l < "$WORK_DIR/recon-payments.txt" | tr -d ' ')"
measure "reconciliation processed_events" "$processed"
measure "seconds after load end until the topic caught up with the outbox" "$catchup_seconds"
measure "seconds after load end until every group had zero lag" "$drain_seconds"
measure "check report" "$(jq -c '{ok, merchants, ledger_entries, processed_events, mismatches: (.mismatches | length)}' "$WORK_DIR/check.json")"
measure "DLQ records (all groups)" "$(dlq_summary | tr '\n' ' ')"

assert_eq "k6 threshold http_req_failed rate<0.01 passed (error rate $err_rate)" true "$thr_err"
assert_eq "k6 threshold http_req_duration p(95)<500 passed (p95 ${p95} ms)" true "$thr_p95"
assert_eq "outbox rows = successful state changes reported by k6" "$k6_transitions" "$rows"
assert_eq "outbox rows = events implied by the API payment states" "$implied" "$rows"
assert_eq "Debezium caught up after the load" 0 "$caught_up_rc"
assert_files_equal "distinct event_ids on the topic = outbox ids" "$WORK_DIR/outbox-ids.txt" "$WORK_DIR/topic-ids.txt"
assert_eq "consumer groups drained after the load" 0 "$drain_rc"
assert_eq "every distinct event applied once (processed_events = outbox rows)" "$rows" "$processed"
assert_files_equal "reconciliation holds every API payment with the same status and amount" "$WORK_DIR/api-payments.txt" "$WORK_DIR/recon-payments.txt"
assert_eq "reconciliation check passes (exit code)" 0 "$check_rc"
assert_eq "nothing dead-lettered" 0 "$(dlq_count "")"

finish load
