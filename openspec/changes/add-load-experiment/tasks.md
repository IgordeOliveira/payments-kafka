# Tasks

## 1. k6 script

- [x] 1.1 Pin a `grafana/k6` image version (an existing tag at least two weeks old) and verify `docker run --rm grafana/k6:<tag> version` prints it
- [x] 1.2 Write `experiments/k6/load.js`: `ramping-arrival-rate` scenario (`RATE`, `RAMP`, `DURATION`), the create → confirm/expire → refund iteration with `MERCHANTS`, `CONFIRM_RATIO`, `REFUND_RATIO`, `RUN_ID` and `API_URL` from `__ENV`, per-transition `Counter`s, thresholds `http_req_failed rate<0.01` and `http_req_duration p(95)<500`, a 30 s request timeout, and `handleSummary` returning JSON on stdout. Verify with a 10-second run (`RATE=5 DURATION=10s RAMP=1s`) piped on stdin to the pinned image on `payments-kafka_default`: it exits 0 and stdout parses with `jq`

## 2. Experiment script and sabotage

- [x] 2.1 Implement the sabotage in `load.sh` as a background `psql` loop that locks `outbox` (`EXCLUSIVE`) for `SABOTAGE_LOCK_SECONDS` every `SABOTAGE_LOCK_SECONDS + SABOTAGE_PAUSE_SECONDS`, stopped when k6 exits and by an EXIT `trap`. Verify that `bash -n` passes and that after a sabotaged run `pg_locks` shows no `ExclusiveLock` on `outbox`
- [x] 2.2 Write `experiments/scripts/load.sh`: `require_stack`, `reset_pipeline`, an optional sabotage recreate with a restoring EXIT `trap`, k6 run capturing the summary and exit code, the Debezium catch-up wait and `wait_drained` with drain-time measurements, then the measurements and assertions from design.md "Assertions" and `finish load`. Verify a short run (`-e RATE=20 -e DURATION=20s`) prints `PASS`
- [x] 2.3 Register `load` in `experiments/bin/experiment` (dispatch and help text) but not in `all`. Verify that `experiments help` lists it and `experiments all` still runs only the four
- [x] 2.4 Calibrate the defaults: run `experiments load` with the defaults on the documented machine and verify `PASS`. If p95 or the error rate fails, lower the default `RATE` and record the measured numbers in `05-load.md`
- [x] 2.5 Calibrate the sabotage: run `-e SABOTAGE=1 experiments load` and verify that it prints `FAIL` only for the k6 threshold assertions, that every consistency assertion passes, and that no lock on `outbox` remains afterwards
- [x] 2.6 Write `experiments/05-load.md` in the style of 01–04 (what it breaks, how it measures, the variables, what to watch in Kafka UI, a sample of measured output, what `SABOTAGE=1` does) and verify that every command in it runs as written

## 3. Documentation

- [x] 3.1 Add the load experiment to the tables in `experiments/README.md` and the root `README.md` (noting that it is not part of `all`), and verify the links resolve
- [x] 3.2 Add one line to `docs/architecture.md` "Docker Compose conventions": experiments may run a pinned `grafana/k6` container on the Compose network. Verify it matches the pinned tag

## 4. Integration

- [x] 4.1 Run `experiments all` after the load experiment, and `experiments load` twice in a row. Verify all print `PASS`, which shows that the load run leaves the stack in a state the other experiments and a repeated run accept
- [x] 4.2 Run `openspec validate add-load-experiment --strict` and verify it passes

## Workflow follow-up

- Open the pull request; the coordinator archives the change after it is merged.
