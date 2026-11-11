#!/usr/bin/env bash
# Exercises every helper once against the running stack with a tiny run
# (30 payments, chaos mode). Not an experiment: it checks the tooling.
source /workspace/experiments/lib/common.sh

require_stack
log "stack is up, connector RUNNING"

reset_pipeline
assert_eq "topics are empty after reset" 0 "$(topic_size "$MAIN_TOPIC")"
assert_eq "reconciliation state is empty after reset" 0 "$(processed_events)"

sim_start SIMULATOR_MODE=chaos SIMULATOR_SEED=1 SIMULATOR_MAX_PAYMENTS=30 SIMULATOR_RATE_PER_SECOND=50 \
  SIMULATOR_CHAOS_DUPLICATE_RATE=0.2 SIMULATOR_CHAOS_INVALID_RATE=0.1 SIMULATOR_CHAOS_OUT_OF_ORDER_RATE=0.1
sim_wait 120 "$WORK_DIR/stats.json"
sent=$(jq .events_sent_total "$WORK_DIR/stats.json")
dups=$(jq .chaos.duplicates "$WORK_DIR/stats.json")
invalid=$(jq .chaos.invalid "$WORK_DIR/stats.json")
measure "simulator events_sent_total" "$sent"
measure "simulator chaos.duplicates / invalid" "$dups / $invalid"

wait_drained 180 $CONSUMER_GROUPS || true
measure "records on $MAIN_TOPIC" "$(topic_size "$MAIN_TOPIC")"
assert_eq "topic records = sent + duplicates + invalid" "$((sent + dups + invalid))" "$(topic_size "$MAIN_TOPIC")"
assert_eq "distinct valid event_ids on topic = events_sent_total" "$sent" "$(distinct_event_ids "$MAIN_TOPIC" | wc -l)"
assert_eq "reconciliation processed_events = events_sent_total" "$sent" "$(processed_events)"

export_balances "$WORK_DIR/balances.json"
measure "merchants in balances export" "$(jq length "$WORK_DIR/balances.json")"
rc=0; recon_check "$WORK_DIR/check.json" || rc=$?
assert_eq "reconciliation check exit code" 0 "$rc"

notifications_since "$RESET_AT" "$WORK_DIR/notifications.ndjson"
measure "notification lines since reset" "$(wc -l < "$WORK_DIR/notifications.ndjson")"
measure "DLQ (reconciliation)" "$(dlq_summary reconciliation | tr '\n' ' ')"
assert_eq "reconciliation DLQ invalid_message = invalid records" "$invalid" "$(dlq_count reconciliation invalid_message)"

sim_stop
finish selftest
