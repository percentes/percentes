#!/usr/bin/env bash
# Process-kill e2e on local Docker: the mock server in a container under the
# on-failure restart policy, one --dry-kill, an N=2 percentes-campaign that
# kills its init process from the host at T_inject, and checks on the
# dry-kill record and the per-run and campaign reports. The host port differs from the config's 18000, which the kind
# cluster holds, and reaches the campaign through --target, --metrics and
# --probe-direct. The config is copied into a derived image, so no host
# path needs to be shared with the Docker VM. On darwin the client CPU gate
# is unmeasured, so the runs are invalid and the check asserts report shape
# only; on Linux a run may be invalid for the client-validity gate alone.
set -euo pipefail

IMAGE=${IMAGE:-percentes/mockserver:dev}
PK_IMAGE=percentes/mockserver-process-kill:e2e
NAME=pct-mock
PORT=${PK_PORT:-18100}
BASE="http://127.0.0.1:$PORT"
OUT=results/process-kill-e2e
CONFIG=configs/process-kill-mock.yaml

say()  { printf '\n== %s\n' "$*"; }
fail() { printf 'PROCESS-KILL-E2E FAIL: %s\n' "$*" >&2; exit 1; }

cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker image inspect "$IMAGE" >/dev/null 2>&1 || fail "image $IMAGE missing; run make docker-build"
VIA=docker-helper
[ "$(uname)" = Linux ] && VIA=sudo
if [ "$VIA" = docker-helper ]; then
  say "pulling busybox (the docker-helper kill path)"
  docker pull -q busybox >/dev/null
fi

say "starting $NAME on $BASE under --restart on-failure"
printf 'FROM %s\nCOPY %s /etc/percentes/run.yaml\n' "$IMAGE" "$(basename "$CONFIG")" |
  docker build -q -t "$PK_IMAGE" -f - "$(dirname "$CONFIG")" >/dev/null
cleanup
docker run -d --name "$NAME" --restart on-failure -p "$PORT:8000" "$PK_IMAGE" >/dev/null
for _ in $(seq 60); do
  curl -sf -o /dev/null --max-time 2 "$BASE/health" && break
  sleep 1
done
curl -sf -o /dev/null --max-time 2 "$BASE/health" || { docker logs "$NAME" 2>&1 | tail -20; fail "$NAME never answered /health on $BASE"; }

BIN="$(mktemp -d)/percentes-campaign"
CGO_ENABLED=0 go build -o "$BIN" ./cmd/percentes-campaign
rm -rf "$OUT" "$OUT-dry"
mkdir -p "$(dirname "$OUT")"

say "dry kill (kill via $VIA)"
"$BIN" \
  --config "$CONFIG" \
  --container "$NAME" \
  --kill-via "$VIA" \
  --target "$BASE" \
  --metrics "$BASE/metrics" \
  --probe-direct "$BASE" \
  --dry-kill \
  --out "$OUT-dry" > "$OUT-dry.log" 2>&1 || { tail -20 "$OUT-dry.log"; fail "dry kill failed"; }
python3 - "$OUT-dry/dry-kill.json" "$(uname)" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    rec = json.load(f)
assert rec["restart_count_advanced"] == 1, f"dry kill: restart count advanced by {rec['restart_count_advanced']}"
assert len(rec["signal_zero"]) == 5, f"dry kill: {len(rec['signal_zero'])} signal-0 brackets"
assert rec["health_ready_after_fire_s"] >= rec["health_calibration_lag_after_fire_s"], "dry kill: /health ready before the calibration began"
if sys.argv[2] == "Linux":
    assert all(b is not None for b in rec["signal_zero_bracket_ms"]), f"dry kill: brackets {rec['signal_zero_bracket_ms']}"
print(f"   dry kill ok: fire error {rec['fire_error_ms']:+.1f} ms, brackets {rec['signal_zero_bracket_ms']} ms")
PY

say "running the N=2 campaign (kill via $VIA)"
set +e
"$BIN" \
  --config "$CONFIG" \
  --container "$NAME" \
  --kill-via "$VIA" \
  --target "$BASE" \
  --metrics "$BASE/metrics" \
  --probe-direct "$BASE" \
  --out "$OUT" \
  --halt-after-invalid-run=false > "$OUT.log" 2>&1
CODE=$?
set -e
tail -4 "$OUT.log" 2>/dev/null || true
[ "$CODE" = "0" ] || [ "$CODE" = "2" ] || { tail -40 "$OUT.log" 2>/dev/null || true; fail "campaign errored (exit $CODE)"; }
[ "$CODE" = "2" ] && echo "   note: exit 2 = at least one run is invalid (on darwin: client CPU gate unmeasured)"

say "verifying the run and campaign reports"
python3 - "$OUT" "$(uname)" "${CI:-}" <<'EOF'
import json, os, re, sys
from datetime import datetime

out, system, ci = sys.argv[1], sys.argv[2], sys.argv[3]

def ts(s):
    # Go writes up to nine fractional digits; datetime takes six.
    s = re.sub(r"\.(\d+)", lambda m: "." + (m.group(1) + "000000")[:6], s.replace("Z", "+00:00"))
    return datetime.fromisoformat(s)

with open(f"{out}/campaign.json") as f:
    rep = json.load(f)
camp = rep["campaign"]
assert camp["repetitions"] == 2 and len(camp["per_run"]) == 2, "two runs published"
for key in ("instrument_commit", "config_sha256"):
    assert rep.get(key), f"campaign.json lacks {key}"
if ci:
    assert not rep["instrument_commit"].endswith("-dirty"), f"dirty build on CI: {rep['instrument_commit']}"

for n in (1, 2):
    assert os.path.exists(f"{out}/run-{n}.txt"), f"run-{n}.txt missing"
    with open(f"{out}/run-{n}.json") as f:
        run = json.load(f)
    assert run.get("config_sha256") == rep["config_sha256"], f"run {n}: config_sha256 differs from the campaign's"
    assert run.get("instrument_commit"), f"run {n}: instrument_commit missing"
    if ci:
        assert not run["instrument_commit"].endswith("-dirty"), f"run {n}: dirty build on CI"
    for w in ("baseline", "guard", "fault"):
        st = run["windows"][w]
        for k in ("completed", "errored", "censored"):
            assert k in st, f"run {n} {w}: {k} missing"
        assert st["completion_incidence"]["points"], f"run {n} {w}: no incidence curve"
        assert "p999_us" in st["e2e_conditional_on_completion"], f"run {n} {w}: no p999"
    o = run["orchestration"]
    err_ms = (ts(o["observed_fire_at"]) - ts(o["planned_fire_at"])).total_seconds() * 1000
    print(f"   run {n}: fire error {err_ms:+.1f} ms, valid={run['run_valid']} {run.get('invalid_reasons') or ''}")
    c = run["container"]
    assert c["after"]["restart_count"] - c["before"]["restart_count"] == 1, f"run {n}: restart count {c['before']['restart_count']} -> {c['after']['restart_count']}"
    if system == "Linux":
        k = c["kill"]
        assert k["remote_before_ns"] > 0 and k["remote_after_ns"] >= k["remote_before_ns"], f"run {n}: no remote fire bracket: {k}"
    assert run["in_flight_at_fire"].get("determinate") is not None, f"run {n}: no determinate in-flight split"
    assert "server_start" in c["boundaries"]["matches"], f"run {n}: mock server_start line not found"
    with open(f"{out}/run-{n}-server.log") as f:
        assert "mockserver: serving on" in f.read(), f"run {n}: server log lacks the mock's start line"
    segs = {s["name"]: s for s in run["decomposition"]["segments"]}
    assert segs["replica_ready"]["measured"], f"run {n}: replica_ready unmeasured: {segs['replica_ready']}"
    tr = segs["traffic_restored"]
    assert not tr["measured"] and tr["note"] == "one replica addressed directly: no Service", f"run {n}: traffic_restored {tr}"
    for row in ("log_bringup", "engine_init", "weight_download", "weight_load", "torch_compile", "profile_kv_capture", "engine_ready", "server_ready"):
        s = segs[row]
        assert not s["measured"] and "pattern" in s["note"], f"run {n}: {row} {s}"
    gates = {g["id"]: g for g in run["validity_gates"]["gates"]}
    assert not gates["G1"]["applicable"], f"run {n}: G1 must be n/a for one replica: {gates['G1']}"
    assert gates["G7"]["applicable"] and gates["G7"]["observed"], f"run {n}: G7 not measured: {gates['G7']}"

ends = {e["name"]: e for e in camp["endpoints"]}
assert ends["outage_s"]["endpoint"] == "primary", f"outage_s {ends['outage_s']}"
assert ends["ttr_equilibrium_s"]["endpoint"] == "not_applicable", f"ttr_equilibrium_s {ends['ttr_equilibrium_s']}"
assert all(r.get("in_flight_loss_fraction") is not None for r in camp["per_run"]), "in_flight_loss_fraction undefined for a run"
if system == "Linux":
    for r in camp["per_run"]:
        other = [x for x in r.get("invalid_reasons") or [] if not x.startswith("client-validity gate failed (\u00a72)")]
        assert not other, f"run {r['run']}: invalid for {other}"
        if r.get("invalid_reasons"):
            with open(f"{out}/run-{r['run']}.json") as f:
                g2 = {g["id"]: g for g in json.load(f)["validity_gates"]["gates"]}["G2"]
            print(f"   run {r['run']}: client-validity gate failed on this host: {g2.get('detail')}")
print(f"   campaign ok: {camp['valid_runs']}/2 valid, outage {[r.get('outage_s') for r in camp['per_run']]} s, "
      f"loss fractions {[round(r['in_flight_loss_fraction'], 3) for r in camp['per_run']]}")
EOF

cleanup
# Captured first: grep -q exiting early breaks the pipe under pipefail.
NAMES=$(docker ps -a --format '{{.Names}}')
grep -qxF "$NAME" <<<"$NAMES" && fail "$NAME still present after cleanup"
say "PROCESS-KILL-E2E PASS (reports in $OUT)"
