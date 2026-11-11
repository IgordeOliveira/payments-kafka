# Shared helpers for the experiments. Sourced by bin/experiment; runs inside the
# `experiments` container (bash, Kafka CLI, kcat, jq, psql, Docker CLI).
#
# Conventions:
#   - every helper prints progress to stderr and results to stdout;
#   - measured values and assertions are collected with `measure` and `assert_*`
#     and printed by `finish`, which exits 0 on PASS and 1 on FAIL;
#   - nothing is sleep-and-hope: waits poll a condition with a timeout.

set -euo pipefail

WORKSPACE=/workspace
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
KAFKA_BIN=/opt/kafka/bin
WORK_DIR="${WORK_DIR:-/tmp/experiment}"
mkdir -p "$WORK_DIR"

MAIN_TOPIC=pix.payments.v1
ALERTS_TOPIC=pix.fraud-alerts.v1
RETRY_TOPIC=pix.payments.retry.v1
DLQ_TOPIC=pix.payments.dlq.v1
ALL_TOPICS="$MAIN_TOPIC $ALERTS_TOPIC $RETRY_TOPIC $DLQ_TOPIC"
CONSUMER_GROUPS="antifraud notification reconciliation"

SIM_CONTAINER=exp-simulator
SIM_URL="http://${SIM_CONTAINER}:8082"

RECON_CLI="node services/reconciliation/dist/cli.js"

# ------------------------------------------------------------------ output

if [ -t 1 ]; then
  C_RED=$'\e[31m'; C_GREEN=$'\e[32m'; C_BOLD=$'\e[1m'; C_DIM=$'\e[2m'; C_RESET=$'\e[0m'
else
  C_RED=''; C_GREEN=''; C_BOLD=''; C_DIM=''; C_RESET=''
fi

ts() { date -u +%H:%M:%S; }
log() { printf '%s[%s]%s %s\n' "$C_DIM" "$(ts)" "$C_RESET" "$*" >&2; }
step() { printf '\n%s==> %s%s\n' "$C_BOLD" "$*" "$C_RESET" >&2; }
die() { printf '%sERROR:%s %s\n' "$C_RED" "$C_RESET" "$*" >&2; exit 2; }

# dc: docker compose on the project, from inside the container. The repository
# is mounted read-only at /workspace; the `name:` in docker-compose.yml keeps
# the project name (payments-kafka) the same as on the host. Only use it for
# services without bind mounts (the consumers and the simulator) with --no-deps,
# so Compose never recreates a container whose host paths it cannot see.
dc() { docker compose -f "$WORKSPACE/docker-compose.yml" --profile apps "$@"; }

# ------------------------------------------------------------------ assertions

MEASURES=()
RESULTS=()
FAILED=0

# measure <name> <value>: record a measured value for the final report.
measure() { MEASURES+=("$1|$2"); log "measured $1 = $2"; }

# assert_eq <description> <expected> <actual>
assert_eq() {
  if [ "$2" = "$3" ]; then
    RESULTS+=("PASS|$1|expected $2, measured $3")
  else
    RESULTS+=("FAIL|$1|expected $2, measured $3")
    FAILED=1
  fi
}

# assert_gt <description> <threshold> <actual>: actual > threshold (integers)
assert_gt() {
  if [ "$3" -gt "$2" ]; then
    RESULTS+=("PASS|$1|expected > $2, measured $3")
  else
    RESULTS+=("FAIL|$1|expected > $2, measured $3")
    FAILED=1
  fi
}

# assert_files_equal <description> <expected file> <actual file>
assert_files_equal() {
  if cmp -s "$2" "$3"; then
    RESULTS+=("PASS|$1|identical ($(wc -c < "$3") bytes, sha256 $(sha256sum "$3" | cut -c1-12))")
  else
    RESULTS+=("FAIL|$1|files differ: diff $2 $3")
    FAILED=1
    diff -u "$2" "$3" | head -40 >&2 || true
  fi
}

# finish <experiment name>: print measured values and assertions, then PASS or
# FAIL with the matching exit status.
finish() {
  local name="$1" entry
  printf '\n%s%s: measured values%s\n' "$C_BOLD" "$name" "$C_RESET"
  for entry in "${MEASURES[@]}"; do
    printf '  %-48s %s\n' "${entry%%|*}" "${entry#*|}"
  done
  printf '\n%s%s: assertions%s\n' "$C_BOLD" "$name" "$C_RESET"
  for entry in "${RESULTS[@]}"; do
    local status="${entry%%|*}" rest="${entry#*|}"
    local color="$C_GREEN"; [ "$status" = PASS ] || color="$C_RED"
    printf '  %s%-4s%s  %s (%s)\n' "$color" "$status" "$C_RESET" "${rest%%|*}" "${rest#*|}"
  done
  echo
  if [ "$FAILED" -eq 0 ] && [ "${#RESULTS[@]}" -gt 0 ]; then
    printf '%s%sPASS%s %s\n' "$C_BOLD" "$C_GREEN" "$C_RESET" "$name"
    exit 0
  fi
  printf '%s%sFAIL%s %s\n' "$C_BOLD" "$C_RED" "$C_RESET" "$name"
  exit 1
}

# wait_until <timeout seconds> <description> <command...>: poll every 2 s.
wait_until() {
  local timeout="$1" what="$2"; shift 2
  local deadline=$((SECONDS + timeout))
  until "$@"; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      log "timed out after ${timeout}s waiting for: $what"
      return 1
    fi
    sleep 2
  done
}

# ------------------------------------------------------------------ stack

container_running() { [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = true ]; }

connector_running() {
  curl -fsS http://kafka-connect:8083/connectors/payments-outbox/status 2>/dev/null \
    | jq -e '.connector.state == "RUNNING" and ([.tasks[].state] | length > 0 and all(. == "RUNNING"))' > /dev/null
}

# require_stack: fail early, with a hint, when the pipeline is not up.
require_stack() {
  local c
  for c in kafka postgres kafka-connect payment-api; do
    container_running "$c" || die "container '$c' is not running. Start the pipeline first: docker compose --profile apps up -d --build --wait"
  done
  for c in antifraud notification reconciliation; do
    [ -n "$(dc ps -a -q "$c" 2>/dev/null)" ] || die "service '$c' has no container. Start the pipeline first: docker compose --profile apps up -d --build --wait"
  done
  connector_running || die "the Debezium connector payments-outbox is not RUNNING (curl -s http://localhost:8083/connectors/payments-outbox/status)"
}

# ------------------------------------------------------------------ kafka

kafka_cli() { local tool="$1"; shift; "$KAFKA_BIN/$tool" --bootstrap-server "$BOOTSTRAP" "$@" 2>/dev/null; }

# topic_dump <topic> [format]: every record of the topic, one per line.
# Default format: partition<TAB>offset<TAB>key<TAB>value.
topic_dump() {
  kcat -C -b "$BOOTSTRAP" -t "$1" -o beginning -e -q -f "${2:-%p\t%o\t%k\t%s\n}" 2>/dev/null
}

# topic_size <topic>: number of records (sum of end offsets; topics start empty).
topic_size() {
  kcat -C -b "$BOOTSTRAP" -t "$1" -o beginning -e -q -f 'x\n' 2>/dev/null | wc -l | tr -d ' '
}

# Contract check for payment events, in jq: the shapes the simulator injects as
# invalid (not JSON, missing event_id, string amount, wrong sequence, unknown
# event_type, USD) all fail it. Input: raw value lines.
JQ_VALID_EVENT='
  fromjson? // empty
  | select(type == "object"
      and (.event_id | type) == "string"
      and (.payment_id | type) == "string"
      and (.merchant_id | type) == "string"
      and .schema_version == 1
      and .currency == "BRL"
      and (.amount_cents | type) == "number" and .amount_cents > 0
      and ({"payment.created":1, "payment.confirmed":2, "payment.expired":2, "payment.refunded":3}[.event_type // ""] == .sequence))'

# distinct_event_ids <topic>: sorted distinct event_id of the valid payment events.
distinct_event_ids() { topic_dump "$1" '%s\n' | jq -rR "$JQ_VALID_EVENT | .event_id" | sort -u; }

# recreate_topics: empties the pipeline topics. Consumers and
# producers must be stopped.
recreate_topics() {
  local t
  for t in $ALL_TOPICS; do
    kafka_cli kafka-topics.sh --delete --if-exists --topic "$t" || true
  done
  topics_gone() { ! kafka_cli kafka-topics.sh --list | grep -qE '^pix\.'; }
  wait_until 60 "topic deletion" topics_gone || die "topics were not deleted"
  # Creation can briefly fail while the broker finishes the deletion: retry.
  local attempt
  for attempt in 1 2 3 4 5 6 7 8 9 10; do
    if KAFKA_BOOTSTRAP_SERVERS="$BOOTSTRAP" sh "$WORKSPACE/infra/kafka/create-topics.sh" > "$WORK_DIR/create-topics.log" 2>&1; then
      return 0
    fi
    sleep 2
  done
  cat "$WORK_DIR/create-topics.log" >&2
  die "could not recreate the topics"
}

# group_state <group>: Stable, Empty, PreparingRebalance, ... and member count.
group_state() {
  kafka_cli kafka-consumer-groups.sh --describe --group "$1" --state \
    | awk -v g="$1" '$1 == g { print $(NF-1), $NF }'
}

# group_stable <group> <members>
group_stable() { [ "$(group_state "$1")" = "Stable $2" ]; }

# group_lag <group...>: total lag per group over every partition it has an
# offset for or is assigned. A partition with data and no committed offset
# counts its whole log as lag. Prints "group total" lines; returns 1 when a
# group is missing, rebalancing or has a partition without an active consumer
# (its lag cannot be trusted then).
group_lag() {
  local out rc=0 g line
  out="$(kafka_cli kafka-consumer-groups.sh --describe $(printf -- '--group %s ' "$@") || true)"
  for g in "$@"; do
    line="$(awk -v g="$g" '
      $1 == g && $3 ~ /^[0-9]+$/ {
        rows++
        if ($4 == "-") lag += $5; else lag += $5 - $4
        if (NF < 7 || $7 == "-") inactive = 1
      }
      END {
        if (rows == 0) print g, "?";
        else print g, lag (inactive ? " inactive" : "")
      }' <<< "$out")"
    echo "$line"
    case "$line" in *'?'* | *inactive*) rc=1 ;; esac
  done
  if grep -qiE 'rebalancing|has no active members' <<< "$out"; then rc=1; fi
  return $rc
}

# wait_drained <timeout> <group...>: waits until every listed group has zero
# lag on all its topics, including the retry topic, on two observations in a
# row a few seconds apart. Retry records that are not due yet stay uncommitted,
# so they count as lag: "drained" includes the retry path.
wait_drained() {
  local timeout="$1"; shift
  local deadline=$((SECONDS + timeout)) zero_streak=0 lags total
  step "Waiting until groups [$*] are drained (lag 0 on main, alert and retry topics; timeout ${timeout}s)"
  while :; do
    if lags="$(group_lag "$@")"; then
      total=$(echo "$lags" | awk '{ s += $2 } END { print s + 0 }')
    else
      total=-1
    fi
    log "lag: $(echo "$lags" | tr '\n' ' ')"
    if [ "$total" = 0 ]; then
      zero_streak=$((zero_streak + 1))
      [ "$zero_streak" -ge 2 ] && { log "drained"; return 0; }
    else
      zero_streak=0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      log "not drained after ${timeout}s"
      return 1
    fi
    sleep 3
  done
}

# dlq_summary [group]: "reason count" lines for DLQ records (optionally of one group).
dlq_summary() {
  topic_dump "$DLQ_TOPIC" '%h\n' \
    | tr ',' '\n' | awk -F= -v g="${1:-}" '
        /^x-dlq-group=/ { group = $2 }
        /^x-dlq-reason=/ { reason = $2 }
        /^x-dlq-attempts=/ { if (g == "" || group == g) count[reason]++; group = ""; reason = "" }
        END { for (r in count) print r, count[r] }' | sort
}

# dlq_count <group> [reason]
dlq_count() {
  dlq_summary "$1" | awk -v r="${2:-}" 'r == "" || $1 == r { s += $2 } END { print s + 0 }'
}

# ------------------------------------------------------------------ consumers and state

recon_cli() { dc run --rm --no-deps -T reconciliation $RECON_CLI "$@"; }

# export_balances <file>: reconciliation's byte-stable balance export.
export_balances() { recon_cli balances > "$1"; }

# recon_check <file>: runs `check`, saves its JSON report, returns its exit code.
recon_check() { local rc=0; recon_cli check > "$1" 2> "$1.stderr" || rc=$?; return $rc; }

psql_recon() { psql -d reconciliation -tAX -c "$1"; }
psql_payments() { psql -d payments -tAX -c "$1"; }
psql_notification() { psql -d notification -tAX -c "$1"; }

processed_events() { psql_recon 'SELECT count(*) FROM processed_events'; }

consumer_containers() { dc ps -q "$1"; }

# notifications_since <timestamp> <file>: every notification line written by
# the notification instances since the timestamp (NDJSON).
notifications_since() {
  local since="$1" out="$2" c
  : > "$out"
  for c in $(dc ps -a -q notification); do
    docker logs --since "$since" "$c" 2>/dev/null | grep '"type":"notification"' >> "$out" || true
  done
}

# stop_producers: the experiment simulator and the default (unbounded) one.
stop_producers() {
  docker rm -f "$SIM_CONTAINER" > /dev/null 2>&1 || true
  if container_running simulator; then
    log "stopping the default simulator (restart it with: docker compose --profile apps start simulator)"
    docker stop simulator > /dev/null
  fi
}

# reset_pipeline [reconciliation instances]: a clean, known state.
#   1. stop the producers (simulators) and every consumer instance;
#   2. delete the consumer groups and empty the four topics (delete, recreate);
#   3. clear reconciliation state (`reset-state --yes`) and notification's
#      dedupe table;
#   4. start the consumers (reconciliation scaled to N) and wait until each
#      group is Stable with all its members.
# The payment API database is not touched: experiments that read it filter by
# time. Prints the reset timestamp (RFC 3339) on stdout.
reset_pipeline() {
  local recon_instances="${1:-1}"
  step "Resetting the pipeline to a clean state"
  stop_producers
  log "stopping consumers"
  dc stop antifraud notification reconciliation > /dev/null 2>&1
  log "deleting consumer groups and recreating topics"
  local g
  for g in $CONSUMER_GROUPS; do
    kafka_cli kafka-consumer-groups.sh --delete --group "$g" > /dev/null || true
  done
  recreate_topics
  log "clearing reconciliation and notification state"
  recon_cli reset-state --yes > /dev/null
  psql_notification 'TRUNCATE sent_notifications' > /dev/null
  RESET_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  log "starting consumers (reconciliation x${recon_instances})"
  dc up -d --no-deps --no-build --scale reconciliation="$recon_instances" antifraud notification reconciliation > /dev/null 2>&1
  wait_until 120 "antifraud group stable" group_stable antifraud 1 || die "antifraud group did not become stable"
  wait_until 120 "notification group stable" group_stable notification 1 || die "notification group did not become stable"
  wait_until 120 "reconciliation group stable" group_stable reconciliation "$recon_instances" || die "reconciliation group did not become stable"
  log "pipeline reset at $RESET_AT"
}

# ------------------------------------------------------------------ simulator

# sim_start <name=value...>: starts a bounded simulator run in container
# exp-simulator (the simulator image, on the Compose network, no host port).
sim_start() {
  local args=() kv
  for kv in "$@"; do args+=(-e "$kv"); done
  docker rm -f "$SIM_CONTAINER" > /dev/null 2>&1 || true
  log "simulator: $*"
  dc run -d --no-deps --name "$SIM_CONTAINER" "${args[@]}" simulator > /dev/null 2>&1
}

sim_stats() { curl -fsS "$SIM_URL/stats"; }

sim_finished() { sim_stats 2>/dev/null | jq -e '.finished == true' > /dev/null; }

# sim_wait <timeout> <stats file>: waits for the run to finish, saves /stats.
sim_wait() {
  local timeout="$1" file="$2"
  step "Waiting for the simulator to finish (timeout ${timeout}s)"
  sim_progress() {
    local s; s="$(sim_stats 2>/dev/null)" || return 1
    log "simulator: $(echo "$s" | jq -c '{steps, events_sent_total, finished}')"
    echo "$s" | jq -e '.finished == true' > /dev/null
  }
  wait_until "$timeout" "simulator finished" sim_progress || die "simulator did not finish in ${timeout}s"
  sim_stats > "$file"
}

sim_stop() { docker rm -f "$SIM_CONTAINER" > /dev/null 2>&1 || true; }
