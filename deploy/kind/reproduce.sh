#!/usr/bin/env bash
# AC7: one-command reproduce from a clean checkout against the local
# cluster. Deploys two mock replicas behind a NodePort Service on kind,
# runs the full harness (load generation, pre-armed fault on a victim
# replica, collection, recovery detection, decomposition probes), and
# verifies the report pair. A run marked invalid by a run-failing gate
# (exit 2) is still a successful REPRODUCTION — the gates are part of the
# instrument — but the reports must exist and be complete.
set -euo pipefail

KIND=${KIND:-kind}
CLUSTER=${CLUSTER:-chaosserve}
IMAGE=${IMAGE:-chaosserve/mockserver:dev}
NS=chaosserve
ADMIN_PORT=18081
OUT=results/kind-e2e

say()  { printf '\n== %s\n' "$*"; }
fail() { printf 'AC7 FAIL: %s\n' "$*" >&2; exit 1; }

PF_PIDS=()
cleanup() { for pid in "${PF_PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT

say "ensuring kind cluster with the NodePort mapping"
if "$KIND" get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  if ! docker port "$CLUSTER-control-plane" 2>/dev/null | grep -q '30800'; then
    echo "   existing cluster lacks the 30800->18000 mapping; recreating"
    "$KIND" delete cluster --name "$CLUSTER"
    "$KIND" create cluster --name "$CLUSTER" --config deploy/kind/kind-config.yaml --wait 120s
  fi
else
  "$KIND" create cluster --name "$CLUSTER" --config deploy/kind/kind-config.yaml --wait 120s
fi

say "building and loading the mock image"
docker build -t "$IMAGE" . >/dev/null
"$KIND" load docker-image "$IMAGE" --name "$CLUSTER"

say "deploying two replicas (config from configs/kind-e2e.yaml)"
kubectl delete namespace "$NS" --ignore-not-found --wait=true
kubectl apply -f deploy/mock/mock.yaml
kubectl -n "$NS" create configmap chaosserve-run-config \
  --from-file=run.yaml=configs/kind-e2e.yaml --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" rollout status deploy/chaosserve-mock --timeout=180s

say "waiting for the NodePort data path"
for _ in $(seq 60); do
  curl -sf -o /dev/null --max-time 2 http://127.0.0.1:18000/health && break
  sleep 1
done
curl -sf -o /dev/null --max-time 2 http://127.0.0.1:18000/health || fail "service not reachable on host port 18000"

VICTIM=$(kubectl -n "$NS" get pods -l app=chaosserve-mock -o jsonpath='{.items[0].metadata.name}')
say "victim replica: $VICTIM (admin via port-forward :$ADMIN_PORT)"
kubectl -n "$NS" port-forward "pod/$VICTIM" "$ADMIN_PORT:8000" >/dev/null 2>&1 &
PF_PIDS+=($!)
for _ in $(seq 30); do
  curl -sf -o /dev/null --max-time 2 "http://127.0.0.1:$ADMIN_PORT/health" && break
  sleep 0.5
done

say "building and running the harness (one config drives the run)"
# Build static (CGO_ENABLED=0) to a FRESH path every run. Two macOS
# lessons encoded here: (a) rebuilding onto a reused path can wedge
# execs in dyld (uninterruptible, unkillable); (b) cgo-linked binaries
# (gopsutil -> IOKit/CoreFoundation) can hit the same dyld wedge on a
# degraded system. CGO_ENABLED=0 keeps Linux CPU-gate sampling fully
# functional (/proc needs no cgo); on darwin the host-CPU gate reports
# unmeasured and therefore does not pass — honest, never silent.
BIN="$(mktemp -d)/chaosserve"
CGO_ENABLED=0 go build -o "$BIN" ./cmd/chaosserve
mkdir -p results
rm -rf "$OUT"
set +e
"$BIN" \
  --config configs/kind-e2e.yaml \
  --out "$OUT" \
  --admin-url "http://127.0.0.1:$ADMIN_PORT" \
  --inject-mode error \
  --inject-duration-s 10 \
  --victim "$VICTIM" \
  --probe-direct "http://127.0.0.1:$ADMIN_PORT" \
  --probe-service "http://127.0.0.1:18000" > "$OUT.log" 2>&1
CODE=$?
set -e
tail -5 "$OUT.log" 2>/dev/null || true
[ "$CODE" = "0" ] || [ "$CODE" = "2" ] || { tail -40 "$OUT.log" 2>/dev/null || true; fail "harness run errored (exit $CODE)"; }
[ "$CODE" = "2" ] && echo "   note: run completed but a run-failing gate marked it invalid (exit 2) — gates are part of the instrument"

say "verifying the report pair"
python3 - "$OUT" <<'EOF'
import json, sys, os
out = sys.argv[1]
assert os.path.exists(f"{out}/report.txt"), "report.txt missing"
with open(f"{out}/report.json") as f:
    rep = json.load(f)
for key in ("config", "config_sha256", "caveat", "conditional_headline",
            "windows", "detector", "in_flight_at_fire", "decomposition", "share_gate"):
    assert key in rep, f"report.json missing {key}"
assert "certifies the instrument" in rep["caveat"]
for w in ("baseline", "fault"):
    assert w in rep["windows"], f"window {w} missing"
    assert rep["windows"][w]["km_curve"]["n"] > 0, f"window {w} KM empty"
det = rep["detector"]
assert len(det["sensitivity"]) == 27, "sensitivity table incomplete"
sg = rep["share_gate"]
assert sg["applicable"], "share gate must be evaluated on the 2-replica topology"
ts = rep.get("orchestration")
assert ts and ts.get("observed_fire_at"), "orchestrated fault must record observed fire"
infl = rep["in_flight_at_fire"]
assert infl["total"] > 0, "in-flight accounting empty"
segs = {s["name"]: s for s in rep["decomposition"]["segments"]}
assert segs["traffic_restored"]["measured"], "traffic-restored probe did not measure"
print(f"   report ok: valid={rep['run_valid']} shares={sg.get('shares')} "
      f"in_flight={infl['total']} fire_err_ok={bool(ts.get('observed_fire_at'))}")
print(f"   headline: {rep['conditional_headline'][:140]}...")
EOF

say "AC7 PASS: one-command reproduce complete (reports in $OUT)"
