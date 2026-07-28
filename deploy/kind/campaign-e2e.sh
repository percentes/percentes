#!/usr/bin/env bash
# Campaign e2e on kind: the §5 repetition pipeline (percentes-campaign) against
# the live 2-replica mock deployment — N runs, per-run §10 validity gates,
# §7 aggregation, campaign report pair. This is the last seam the unit
# suite cannot cover: the campaign engine driving real runs on a real
# cluster. Expected exit is 2 (runs are honestly invalid pre-hardware:
# G5 has no GPU fingerprint, and on darwin the client CPU gate is
# unmeasured); the reproduction PASSES on report completeness, gates
# doing their job is correct behavior.
set -euo pipefail

KIND=${KIND:-kind}
CLUSTER=${CLUSTER:-percentes}
IMAGE=${IMAGE:-percentes/mockserver:dev}
NS=percentes
ADMIN_PORT=18082
OUT=results/kind-campaign

say()  { printf '\n== %s\n' "$*"; }
fail() { printf 'CAMPAIGN-E2E FAIL: %s\n' "$*" >&2; exit 1; }

PF_PIDS=()
cleanup() { for pid in "${PF_PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT

say "ensuring cluster, image, and deployment (config from configs/kind-campaign.yaml)"
if ! "$KIND" get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  "$KIND" create cluster --name "$CLUSTER" --config deploy/kind/kind-config.yaml --wait 120s
fi
docker build -t "$IMAGE" . >/dev/null
"$KIND" load docker-image "$IMAGE" --name "$CLUSTER"
kubectl delete namespace "$NS" --ignore-not-found --wait=true
kubectl apply -f deploy/mock/mock.yaml
kubectl -n "$NS" create configmap percentes-run-config \
  --from-file=run.yaml=configs/kind-campaign.yaml --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" rollout status deploy/percentes-mock --timeout=180s
for _ in $(seq 60); do
  curl -sf -o /dev/null --max-time 2 http://127.0.0.1:18000/health && break
  sleep 1
done
curl -sf -o /dev/null --max-time 2 http://127.0.0.1:18000/health || fail "NodePort data path not reachable"

VICTIM=$(kubectl -n "$NS" get pods -l app=percentes-mock -o jsonpath='{.items[0].metadata.name}')
say "victim replica: $VICTIM"
kubectl -n "$NS" port-forward "pod/$VICTIM" "$ADMIN_PORT:8000" >/dev/null 2>&1 &
PF_PIDS+=($!)
for _ in $(seq 30); do
  curl -sf -o /dev/null --max-time 2 "http://127.0.0.1:$ADMIN_PORT/health" && break
  sleep 0.5
done

say "running the N=2 campaign (fresh-path CGO_ENABLED=0 binary; ~5 min)"
BIN="$(mktemp -d)/percentes-campaign"
CGO_ENABLED=0 go build -o "$BIN" ./cmd/percentes-campaign
rm -rf "$OUT"
mkdir -p "$(dirname "$OUT")"
set +e
"$BIN" \
  --config configs/kind-campaign.yaml \
  --out "$OUT" \
  --admin-url "http://127.0.0.1:$ADMIN_PORT" \
  --inject-mode error \
  --inject-duration-s 10 \
  --victim "$VICTIM" > "$OUT.log" 2>&1
CODE=$?
set -e
tail -4 "$OUT.log" 2>/dev/null || true
[ "$CODE" = "0" ] || [ "$CODE" = "2" ] || { tail -40 "$OUT.log" 2>/dev/null || true; fail "campaign errored (exit $CODE)"; }
[ "$CODE" = "2" ] && echo "   note: exit 2 = at least one run-validity gate failed (expected pre-hardware: G5 unobserved; darwin CPU unmeasured)"

say "verifying the campaign report"
python3 - "$OUT" <<'EOF'
import json, sys, os
out = sys.argv[1]
assert os.path.exists(f"{out}/campaign.txt"), "campaign.txt missing"
with open(f"{out}/campaign.json") as f:
    rep = json.load(f)
camp = rep["campaign"]
assert camp["repetitions"] == 2 and len(camp["per_run"]) == 2, "must publish every run verbatim (§5)"
names = {e["name"] for e in camp["endpoints"]}
for want in ("ttr_equilibrium_s", "in_flight_loss_fraction", "survivor_p95_ms", "integrated_goodput_deficit"):
    assert want in names, f"endpoint {want} missing"
lf = next(e for e in camp["endpoints"] if e["name"] == "in_flight_loss_fraction")
assert lf["contributing_n"] == 2, f"both runs are victim-attributed, got {lf['contributing_n']}"
assert camp.get("noise_floor_cov") is None, "noise floor label is clean_delete-only (§7); mock variant must not carry it"
gates = rep["validity_gates"]
assert len(gates) == 2, "one §10 gate report per run"
for i, g in enumerate(gates):
    ids = {x["id"]: x for x in g["gates"]}
    assert set(ids) == {"G1","G2","G3","G4","G5","G6"}, f"run {i}: gate set incomplete"
    assert ids["G5"]["pass"] is False and ids["G5"]["observed"] is False, "G5 must fail-unobserved without GPU fingerprints"
    assert ids["G1"]["pass"] is True, f"run {i}: share gate should pass on the 2-replica deployment: {ids['G1']}"
per = camp["per_run"]
assert all(r["in_flight_loss_fraction"] is not None for r in per), "victim-scoped loss must be present per run"
print(f"   campaign ok: {camp['valid_runs']}/2 fully-valid runs (expected 0 pre-hardware), "
      f"loss fractions {[round(r['in_flight_loss_fraction'],3) for r in per]}")
EOF

say "CAMPAIGN-E2E PASS (reports in $OUT)"
