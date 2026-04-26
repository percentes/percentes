#!/usr/bin/env bash
# In-cluster smoke suite for the mock server on kind. Proves: image runs
# from the ConfigMap-mounted run config, two replicas spread across worker
# nodes behind one Service, SSE streaming works through the Service, the
# per-replica request counters are exposed, and a fault armed on one pod
# fires with recorded timestamps and expires on schedule.
set -euo pipefail

KIND=${KIND:-kind}
CLUSTER=${CLUSTER:-chaosserve}
IMAGE=${IMAGE:-chaosserve/mockserver:dev}
NS=chaosserve
SVC_PORT=18080
POD_PORT=18081

say()  { printf '\n== %s\n' "$*"; }
fail() { printf 'SMOKE FAIL: %s\n' "$*" >&2; exit 1; }

PF_PIDS=()
cleanup() {
  for pid in "${PF_PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

# Wait until a port-forward answers.
wait_http() { # url attempts
  local url=$1 attempts=${2:-30}
  for _ in $(seq "$attempts"); do
    if curl -sf -o /dev/null --max-time 2 "$url"; then return 0; fi
    sleep 0.5
  done
  return 1
}

say "loading image into kind cluster '$CLUSTER'"
"$KIND" load docker-image "$IMAGE" --name "$CLUSTER"

say "deploying mock (fresh namespace, config from configs/kind-e2e.yaml)"
kubectl delete namespace "$NS" --ignore-not-found --wait=true
kubectl apply -f deploy/mock/mock.yaml
kubectl -n "$NS" create configmap chaosserve-run-config \
  --from-file=run.yaml=configs/kind-e2e.yaml --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" rollout status deploy/chaosserve-mock --timeout=180s

say "asserting replica placement"
READY=$(kubectl -n "$NS" get deploy chaosserve-mock -o jsonpath='{.status.readyReplicas}')
[ "$READY" = "2" ] || fail "expected 2 ready replicas, got ${READY:-0}"
CLUSTER_NODES=$(kubectl get nodes --no-headers | wc -l | tr -d ' ')
SPREAD=$(kubectl -n "$NS" get pods -l app=chaosserve-mock -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u | wc -l | tr -d ' ')
if [ "$CLUSTER_NODES" -ge 2 ] && [ "$SPREAD" != "2" ]; then
  fail "multi-node cluster but replicas share a node (anti-affinity not honored)"
fi
echo "   replicas=2 across $SPREAD node(s) ($CLUSTER_NODES in cluster)"

say "SSE streaming through the Service"
kubectl -n "$NS" port-forward svc/chaosserve-mock "$SVC_PORT:8000" >/dev/null 2>&1 &
PF_PIDS+=($!)
wait_http "http://127.0.0.1:$SVC_PORT/health" || fail "service port-forward never became ready"

SSE=$(curl -sN --max-time 20 -H 'Content-Type: application/json' \
  -d '{"model":"mock","messages":[{"role":"user","content":"hi"}],"stream":true,"max_tokens":4,"ignore_eos":true}' \
  "http://127.0.0.1:$SVC_PORT/v1/chat/completions")
DATA_LINES=$(printf '%s' "$SSE" | grep -c '^data: ') || true
printf '%s' "$SSE" | grep -q 'data: \[DONE\]' || fail "SSE stream missing [DONE]"
# 4 content chunks + 1 finish chunk + [DONE] = 6 data lines
[ "$DATA_LINES" = "6" ] || fail "expected 6 SSE data lines (4 tokens + finish + DONE), got $DATA_LINES"

say "per-replica metrics and admin-armed fault on one pod"
POD=$(kubectl -n "$NS" get pods -l app=chaosserve-mock -o jsonpath='{.items[0].metadata.name}')
kubectl -n "$NS" port-forward "pod/$POD" "$POD_PORT:8000" >/dev/null 2>&1 &
PF_PIDS+=($!)
wait_http "http://127.0.0.1:$POD_PORT/health" || fail "pod port-forward never became ready"

curl -sf "http://127.0.0.1:$POD_PORT/metrics" | grep -q 'chaosserve_mock_' \
  || fail "per-replica Prometheus counters not exposed"

ARM=$(curl -sf -X POST -H 'Content-Type: application/json' \
  -d '{"mode":"error","delay_s":0,"duration_s":3}' \
  "http://127.0.0.1:$POD_PORT/admin/faults")
printf '%s' "$ARM" | grep -q '"mode":"error"' || fail "arming fault failed: $ARM"

sleep 1
MID=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 -H 'Content-Type: application/json' \
  -d '{"model":"mock","messages":[{"role":"user","content":"hi"}],"stream":true,"max_tokens":2,"ignore_eos":true}' \
  "http://127.0.0.1:$POD_PORT/v1/chat/completions")
[ "$MID" = "500" ] || fail "expected 500 during error window on faulted pod, got $MID"

sleep 3
POST=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -H 'Content-Type: application/json' \
  -d '{"model":"mock","messages":[{"role":"user","content":"hi"}],"stream":true,"max_tokens":2,"ignore_eos":true}' \
  "http://127.0.0.1:$POD_PORT/v1/chat/completions")
[ "$POST" = "200" ] || fail "expected 200 after fault expiry, got $POST"

say "verifying recorded cluster pins against the live cluster"
K8S_ACTUAL=$(kubectl version -o json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"])')
K8S_PINNED=$(kubectl -n "$NS" get cm chaosserve-run-config -o jsonpath='{.data.run\.yaml}' \
  | awk '/^  kubernetes:/{f=1;next} f&&/version:/{print $2; exit}')
[ "$K8S_ACTUAL" = "$K8S_PINNED" ] \
  || fail "pins.kubernetes.version ($K8S_PINNED) does not record the live cluster ($K8S_ACTUAL)"
NMGP_PINNED=$(kubectl -n "$NS" get cm chaosserve-run-config -o jsonpath='{.data.run\.yaml}' \
  | awk '/node_monitor_grace_period_s:/{print $2; exit}')
NMGP_FLAG=$(kubectl -n kube-system get pod -l component=kube-controller-manager \
  -o jsonpath='{.items[0].spec.containers[0].command}' \
  | grep -o 'node-monitor-grace-period=[0-9smh]*' | cut -d= -f2 || true)
# Flag absent means the compiled default applies: 40s on the pinned v1.30.
NMGP_ACTUAL_S=$( [ -n "$NMGP_FLAG" ] && printf '%s' "${NMGP_FLAG%s}" || printf '40' )
[ "$NMGP_ACTUAL_S" = "$NMGP_PINNED" ] \
  || fail "pins.kubernetes.node_monitor_grace_period_s ($NMGP_PINNED) does not record the cluster's actual value (${NMGP_ACTUAL_S}s)"
echo "   pins verified: kubernetes $K8S_ACTUAL, node-monitor-grace-period ${NMGP_ACTUAL_S}s"

say "fault lifecycle timestamps recorded"
python3 - "$POD_PORT" <<'EOF'
import json, sys, urllib.request
port = sys.argv[1]
with urllib.request.urlopen(f"http://127.0.0.1:{port}/admin/faults", timeout=5) as r:
    state = json.load(r)
faults = state["faults"]
assert len(faults) == 1, f"expected 1 fault record, got {len(faults)}"
f = faults[0]
assert f["state"] == "expired", f"fault not expired: {f}"
for key in ("armed_at", "fired_at", "expired_at", "armed_offset_s", "fired_offset_s", "expired_offset_s"):
    assert f.get(key) is not None, f"missing {key}: {f}"
dur = f["expired_offset_s"] - f["fired_offset_s"]
assert 2.5 <= dur <= 4.0, f"fault window duration off: {dur}s (wanted ~3s)"
print(f"   fault lifecycle ok: fired at +{f['fired_offset_s']:.2f}s, expired at +{f['expired_offset_s']:.2f}s")
EOF

say "SMOKE PASS"
