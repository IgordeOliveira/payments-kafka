#!/usr/bin/env bash
# Experiment 2: a consumer killed mid-processing loses nothing. See experiments/02-kill-consumer.md.
source /workspace/experiments/lib/common.sh

SEED="${SEED:-202}"
PAYMENTS="${PAYMENTS:-2000}"
RATE="${RATE:-300}"
INSTANCES="${INSTANCES:-3}"
KILL_AFTER_EVENTS="${KILL_AFTER_EVENTS:-500}"

require_stack
reset_pipeline "$INSTANCES"

step "Bounded chaos run: seed $SEED, $PAYMENTS payments at $RATE steps/s, reconciliation x$INSTANCES"
sim_start SIMULATOR_MODE=chaos SIMULATOR_SEED="$SEED" SIMULATOR_MAX_PAYMENTS="$PAYMENTS" SIMULATOR_RATE_PER_SECOND="$RATE"

step "Waiting until reconciliation applied $KILL_AFTER_EVENTS events, then SIGKILL one instance"
enough_applied() { [ "$(processed_events)" -ge "$KILL_AFTER_EVENTS" ]; }
wait_until 120 "reconciliation applied $KILL_AFTER_EVENTS events" enough_applied || die "reconciliation did not start applying events"
victim=$(consumer_containers reconciliation | shuf -n 1)
victim_name=$(docker inspect -f '{{.Name}}' "$victim" | tr -d /)
docker kill --signal KILL "$victim" > /dev/null
killed_at_processed=$(processed_events)
sim_running_at_kill=$(sim_stats | jq .running)
log "killed $victim_name (SIGKILL) with $killed_at_processed events applied; simulator still running: $sim_running_at_kill"
measure "killed instance" "$victim_name"
measure "events applied when killed" "$killed_at_processed"
measure "simulator running when killed" "$sim_running_at_kill"

sleep 5
lag_after_kill=$(group_lag reconciliation | awk '{ print $2 }' || true)
measure "reconciliation lag 5 s after the kill" "$lag_after_kill"
log "restarting $victim_name (it rejoins the group as a new member)"
docker start "$victim" > /dev/null

sim_wait 300 "$WORK_DIR/stats.json"
drain_rc=0
wait_drained 300 reconciliation || drain_rc=$?
sim_stop

if [ "${SABOTAGE:-0}" = 1 ]; then
  step "SABOTAGE: forgetting one applied event, as if the kill had lost it"
  psql_recon "DELETE FROM processed_events WHERE event_id = (SELECT event_id FROM processed_events ORDER BY event_id LIMIT 1)" > /dev/null
fi

sent=$(jq .events_sent_total "$WORK_DIR/stats.json")
invalid=$(jq .chaos.invalid "$WORK_DIR/stats.json")
processed=$(processed_events)
on_topic=$(distinct_event_ids "$MAIN_TOPIC" | wc -l)
dup_logs=0
for c in $(consumer_containers reconciliation); do
  n=$(docker logs --since "$RESET_AT" "$c" 2>/dev/null | grep -c '"msg":"duplicate event ignored"' || true)
  dup_logs=$((dup_logs + n))
done
injected=$(jq .chaos.duplicates "$WORK_DIR/stats.json")
measure "simulator events_sent_total (distinct valid events)" "$sent"
measure "simulator chaos duplicates / out_of_order / invalid" "$(jq -r '"\(.chaos.duplicates) / \(.chaos.out_of_order) / \(.chaos.invalid)"' "$WORK_DIR/stats.json")"
measure "distinct valid event_ids on $MAIN_TOPIC" "$on_topic"
measure "reconciliation processed_events" "$processed"
measure "'duplicate event ignored' log lines" "$dup_logs"
# Duplicates beyond the injected ones were redelivered after their database
# commit (the kill came before the offset commit). A record whose transaction
# was rolled back by the kill is applied normally on redelivery and is not
# visible here.
measure "redelivered after their DB commit (log lines - injected duplicates)" "$((dup_logs - injected))"
measure "DLQ records (reconciliation)" "$(dlq_summary reconciliation | tr '\n' ' ')"
check_rc=0; recon_check "$WORK_DIR/check.json" || check_rc=$?
measure "check report" "$(jq -c '{ok, merchants, ledger_entries, processed_events, mismatches: (.mismatches | length)}' "$WORK_DIR/check.json")"

assert_gt "the instance was killed mid-run (events applied at kill > 0)" 0 "$killed_at_processed"
assert_gt "the instance was killed mid-run (events still to apply at kill > 0)" "$killed_at_processed" "$sent"
assert_eq "reconciliation group drained after the kill (lag 0 incl. retry topic)" 0 "$drain_rc"
assert_eq "topic holds every event the simulator produced (distinct valid event_ids)" "$sent" "$on_topic"
assert_eq "every distinct event applied exactly once (processed_events)" "$sent" "$processed"
assert_eq "reconciliation check passes (exit code)" 0 "$check_rc"
assert_eq "reconciliation DLQ holds only the invalid records" "$invalid" "$(dlq_count reconciliation invalid_message)"
assert_eq "no valid event dead-lettered by reconciliation" 0 "$(( $(dlq_count reconciliation) - $(dlq_count reconciliation invalid_message) ))"

log "scaling reconciliation back to 1 instance"
dc up -d --no-deps --no-build --scale reconciliation=1 reconciliation > /dev/null 2>&1 || true

finish kill-consumer
