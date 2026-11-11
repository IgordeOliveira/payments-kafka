#!/usr/bin/env bash
# Experiment 1: duplicated events are applied once. See experiments/01-duplicates.md.
source /workspace/experiments/lib/common.sh

SEED="${SEED:-101}"
PAYMENTS="${PAYMENTS:-400}"
RATE="${RATE:-100}"
DUPLICATE_RATE="${DUPLICATE_RATE:-0.2}"

require_stack

# --- Run A: the stream with chaos (duplicates, out-of-order, invalid records)
reset_pipeline
step "Run A: seed $SEED, $PAYMENTS payments, duplicate rate $DUPLICATE_RATE (plus out-of-order and invalid records)"
sim_start SIMULATOR_MODE=chaos SIMULATOR_SEED="$SEED" SIMULATOR_MAX_PAYMENTS="$PAYMENTS" SIMULATOR_RATE_PER_SECOND="$RATE" \
  SIMULATOR_CHAOS_DUPLICATE_RATE="$DUPLICATE_RATE" SIMULATOR_CHAOS_OUT_OF_ORDER_RATE=0.05 SIMULATOR_CHAOS_INVALID_RATE=0.02
sim_wait 300 "$WORK_DIR/stats-a.json"
wait_drained 300 $CONSUMER_GROUPS || true

if [ "${SABOTAGE:-0}" = 1 ]; then
  step "SABOTAGE: applying one duplicate a second time, as a consumer without dedupe would"
  psql_recon "UPDATE merchant_balances b SET balance_cents = b.balance_cents + l.amount_cents, confirmed_count = b.confirmed_count + 1
              FROM (SELECT merchant_id, amount_cents FROM ledger_entries WHERE entry_type = 'credit' ORDER BY event_id LIMIT 1) l
              WHERE b.merchant_id = l.merchant_id" > /dev/null
fi

sent=$(jq .events_sent_total "$WORK_DIR/stats-a.json")
dups=$(jq .chaos.duplicates "$WORK_DIR/stats-a.json")
invalid=$(jq .chaos.invalid "$WORK_DIR/stats-a.json")
notifiable=$(jq '.events_sent["payment.confirmed"] + .events_sent["payment.expired"] + .events_sent["payment.refunded"]' "$WORK_DIR/stats-a.json")
measure "A: simulator events_sent_total (distinct events)" "$sent"
measure "A: simulator chaos.duplicates" "$dups"
measure "A: simulator chaos.out_of_order / invalid" "$(jq .chaos.out_of_order "$WORK_DIR/stats-a.json") / $invalid"
measure "A: records on $MAIN_TOPIC" "$(topic_size "$MAIN_TOPIC")"
measure "A: distinct valid event_ids on $MAIN_TOPIC" "$(distinct_event_ids "$MAIN_TOPIC" | wc -l)"
processed=$(processed_events)
measure "A: reconciliation processed_events" "$processed"
dup_logs=0
for c in $(consumer_containers reconciliation); do
  n=$(docker logs --since "$RESET_AT" "$c" 2>/dev/null | grep -c '"msg":"duplicate event ignored"' || true)
  dup_logs=$((dup_logs + n))
done
measure "A: reconciliation 'duplicate event ignored' log lines" "$dup_logs"
measure "A: DLQ records (reconciliation)" "$(dlq_summary reconciliation | tr '\n' ' ')"
dlq_invalid=$(dlq_count reconciliation invalid_message)
dlq_other=$(( $(dlq_count reconciliation) - dlq_invalid ))

check_rc=0; recon_check "$WORK_DIR/check-a.json" || check_rc=$?
measure "A: check report" "$(jq -c '{ok, merchants, ledger_entries, processed_events, mismatches: (.mismatches | length)}' "$WORK_DIR/check-a.json")"
export_balances "$WORK_DIR/balances-a.json"

notifications_since "$RESET_AT" "$WORK_DIR/notifications-a.ndjson"
payment_notes=$(jq -r 'select(.kind != "fraud_alert") | .notification_key' "$WORK_DIR/notifications-a.ndjson" | wc -l)
fraud_notes=$(jq -r 'select(.kind == "fraud_alert") | .notification_key' "$WORK_DIR/notifications-a.ndjson" | wc -l)
total_notes=$(wc -l < "$WORK_DIR/notifications-a.ndjson")
distinct_notes=$(jq -r .notification_key "$WORK_DIR/notifications-a.ndjson" | sort -u | wc -l)
distinct_alerts=$(topic_dump "$ALERTS_TOPIC" '%s\n' | jq -r '.alert_id' | sort -u | wc -l)
measure "A: notification lines (payment / fraud_alert)" "$total_notes ($payment_notes / $fraud_notes)"
measure "A: distinct alert_ids on $ALERTS_TOPIC" "$distinct_alerts"

# --- Run B: the same seed with every chaos rate at 0 (the duplicate-free stream)
reset_pipeline
step "Run B (reference): seed $SEED, $PAYMENTS payments, no chaos"
sim_start SIMULATOR_MODE=chaos SIMULATOR_SEED="$SEED" SIMULATOR_MAX_PAYMENTS="$PAYMENTS" SIMULATOR_RATE_PER_SECOND="$RATE" \
  SIMULATOR_CHAOS_DUPLICATE_RATE=0 SIMULATOR_CHAOS_OUT_OF_ORDER_RATE=0 SIMULATOR_CHAOS_INVALID_RATE=0
sim_wait 300 "$WORK_DIR/stats-b.json"
wait_drained 300 reconciliation || true
sim_stop
measure "B: simulator events_sent_total / duplicates" "$(jq .events_sent_total "$WORK_DIR/stats-b.json") / $(jq .chaos.duplicates "$WORK_DIR/stats-b.json")"
measure "B: reconciliation processed_events" "$(processed_events)"
export_balances "$WORK_DIR/balances-b.json"
measure "balances A / B (merchants)" "$(jq length "$WORK_DIR/balances-a.json") / $(jq length "$WORK_DIR/balances-b.json")"

# --- Assertions
assert_gt "the simulator injected duplicates" 0 "$dups"
assert_eq "runs A and B produced the same lifecycle events" \
  "$(jq -c .events_sent "$WORK_DIR/stats-b.json")" "$(jq -c .events_sent "$WORK_DIR/stats-a.json")"
assert_eq "A: each distinct event applied once (processed_events = events_sent_total)" "$sent" "$processed"
assert_eq "A: reconciliation check passes (exit code)" 0 "$check_rc"
assert_files_equal "A balances = B balances (same seed, no duplicates)" "$WORK_DIR/balances-b.json" "$WORK_DIR/balances-a.json"
assert_eq "A: payment notifications = distinct notifiable events (confirmed+expired+refunded)" "$notifiable" "$payment_notes"
assert_eq "A: no notification key emitted twice (distinct keys = lines)" "$total_notes" "$distinct_notes"
assert_eq "A: fraud notifications = distinct alert_ids" "$distinct_alerts" "$fraud_notes"
assert_eq "A: reconciliation DLQ holds only the invalid records" "$invalid" "$dlq_invalid"
assert_eq "A: no valid event dead-lettered by reconciliation" 0 "$dlq_other"

finish duplicates
