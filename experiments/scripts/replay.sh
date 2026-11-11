#!/usr/bin/env bash
# Experiment 4: reconciliation is rebuilt from the topic and reaches identical
# balances. See experiments/04-replay.md.
source /workspace/experiments/lib/common.sh

SEED="${SEED:-404}"
PAYMENTS="${PAYMENTS:-800}"
RATE="${RATE:-100}"

require_stack
reset_pipeline

step "Building state: chaos run, seed $SEED, $PAYMENTS payments (duplicates, out-of-order and invalid records at the default rates)"
sim_start SIMULATOR_MODE=chaos SIMULATOR_SEED="$SEED" SIMULATOR_MAX_PAYMENTS="$PAYMENTS" SIMULATOR_RATE_PER_SECOND="$RATE"
sim_wait 300 "$WORK_DIR/stats.json"
sim_stop
wait_drained 300 reconciliation || die "reconciliation did not drain before the export"

sent=$(jq .events_sent_total "$WORK_DIR/stats.json")
export_balances "$WORK_DIR/balances-before.json"
before_processed=$(processed_events)
check_before_rc=0; recon_check "$WORK_DIR/check-before.json" || check_before_rc=$?
measure "simulator events_sent_total" "$sent"
measure "records on $MAIN_TOPIC" "$(topic_size "$MAIN_TOPIC")"
measure "before: processed_events / merchants" "$before_processed / $(jq length "$WORK_DIR/balances-before.json")"
measure "before: export sha256" "$(sha256sum "$WORK_DIR/balances-before.json" | cut -c1-16)"

step "1. Stopping every reconciliation instance"
dc stop reconciliation > /dev/null 2>&1
group_empty() { [ "$(group_state reconciliation)" = "Empty 0" ]; }
wait_until 60 "reconciliation group empty" group_empty || die "the reconciliation group still has members"

step "2. Clearing reconciliation state (reset-state --yes)"
recon_cli reset-state --yes > /dev/null
measure "after reset-state: processed_events" "$(processed_events)"

if [ "${SABOTAGE:-0}" = 1 ]; then
  step "3. SABOTAGE: rewinding to offset 10 of each partition instead of the earliest offset"
  reset_args=(--to-offset 10)
else
  step "3. Rewinding the group to the earliest offset of $MAIN_TOPIC"
  reset_args=(--to-earliest)
fi
kafka_cli kafka-consumer-groups.sh --group reconciliation --reset-offsets "${reset_args[@]}" --topic "$MAIN_TOPIC" --execute \
  | awk 'NR == 1 || $1 == "reconciliation"' >&2
measure "committed offsets after the rewind (sum over $MAIN_TOPIC)" \
  "$(kafka_cli kafka-consumer-groups.sh --describe --group reconciliation | awk -v t="$MAIN_TOPIC" '$2 == t { s += $4 } END { print s + 0 }')"

step "4. Starting reconciliation again: it reprocesses the whole topic"
dc up -d --no-deps --no-build reconciliation > /dev/null 2>&1
wait_until 120 "reconciliation group stable" group_stable reconciliation 1 || die "reconciliation did not rejoin"
drain_rc=0
wait_drained 300 reconciliation || drain_rc=$?

export_balances "$WORK_DIR/balances-after.json"
after_processed=$(processed_events)
check_after_rc=0; recon_check "$WORK_DIR/check-after.json" || check_after_rc=$?
measure "after: processed_events / merchants" "$after_processed / $(jq length "$WORK_DIR/balances-after.json")"
measure "after: export sha256" "$(sha256sum "$WORK_DIR/balances-after.json" | cut -c1-16)"

assert_gt "the export before the replay is not empty (merchants)" 0 "$(jq length "$WORK_DIR/balances-before.json")"
assert_eq "before: every distinct event applied (processed_events)" "$sent" "$before_processed"
assert_eq "before: check passes (exit code)" 0 "$check_before_rc"
assert_eq "replay drained (lag 0 incl. retry topic)" 0 "$drain_rc"
assert_eq "after: every distinct event applied again (processed_events)" "$sent" "$after_processed"
assert_eq "after: check passes (exit code)" 0 "$check_after_rc"
assert_files_equal "balances after the replay are byte-identical to before" "$WORK_DIR/balances-before.json" "$WORK_DIR/balances-after.json"

finish replay
