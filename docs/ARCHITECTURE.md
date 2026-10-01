# Percentes architecture: system map and trace index

This document maps the whole system: what each piece does, why it exists
(with the SPEC.md clause it serves), how data flows from a scheduled
request to a number in a report, and how to take any published number
and trace it back to the line of code that produced it. Read it top to
bottom once; after that, the trace tables in §5 are the working index.

SPEC.md is authoritative everywhere.

## 1. Overview

Percentes measures large language model (LLM) inference reliability
under load and failure.
Replica loss is the Phase 0/1 fault class: what happens when a
Kubernetes-served LLM inference service loses a replica under sustained
load: the three questions SPEC.md §1 pins. Phase 0 builds and certifies the *instrument*
against a mock inference server on a local kind cluster; passing the
acceptance suite says nothing about real graphics processing unit (GPU)
behaviour.
The Phase 1 groundwork adds everything for the real experiment that
can be verified without hardware; one calibration has run against a
standalone container, and the in-cluster run is pending.

The core methodological commitments:

- **Open loop**: requests are dispatched on a schedule fixed before the
  run. The generator never slows down because the system did (coordinated
  omission, SPEC §2).
- **Three-state outcomes** (§3): every scheduled request ends
  in exactly one of completed / errored / censored. Only completions enter
  latency histograms; failures are first-class rates; the completion curve
  is the Aalen-Johansen cumulative incidence, in which errors compete and
  only censored requests (no terminal event by the pinned 30 s timeout)
  are censored observations, never latency percentiles.
- **Pre-registered numbers**: the values SPEC.md pins are enforced at
  config-load time (SPEC.md §0 states the pledge; `internal/config`
  enforces it); calibration-derived values (lambda_max, lambda_r) are
  recorded in the config's `calibration` block once §10 has run, and
  the Phase 1 infrastructure pins land with the Phase 1 schema.
- **Unmeasured reports as unmeasured**: anything unmeasured is reported
  as unmeasured-and-failing (gates) or N/A-with-reason (segments,
  equilibrium).

## 2. Lifecycle of one request

The diagram's columns are fixed width, so its terms are glossed here:
server-sent events (SSE) carry the stream; time to first token (TTFT)
and inter-token latency (ITL) are the two latencies, both re-based to the
intended dispatch time; HTTP is the Hypertext Transfer Protocol and RST
a TCP connection reset.

```
 schedule.go            sse_client.go                    mock / vLLM
 ───────────            ─────────────                    ───────────
 t_i fixed before run → worker wakes at t_i (timer+spin) → POST /v1/chat/completions (SSE)
                        dispatch recorded (skew = dispatch−t_i)
                        first content chunk → FirstTokNs          TTFT = FirstTok − t_i
                        per-token gaps → ITLsUs[]                 (re-based to INTENDED time)
                        terminal event:
                          data:[DONE], content seen      → completed, DoneNs   e2e = Done − t_i
                          data:[DONE], no content        → errored (empty_stream)
                          HTTP 429                       → errored (status_429)
                          non-200/RST/bad SSE/other fail → errored (+class)
                          ctx deadline 30s               → censored
```

Downstream, `collect.Collect` assigns the request to the window of its
*intended* time (windows never straddle T_inject, §3; the baseline window
ends one pinned client timeout before the fire anchor and the guard window
runs from there to T_inject), records completed
latencies into the pinned HdrHistogram configuration via `RecordValue()`
only, adds every request to that window's incidence curve, and accumulates
failure rates, goodput, the §4 threshold sweep, and §7 tail confidence
intervals (CIs).
`detect.BuildSeries` buckets the same requests at 1 Hz for the recovery
detector. Nothing is computed twice from different sources: report
numbers come from these artifacts.

## 2.1 The pacer timing model (how dispatch stays a pure function of the clock)

The load generator's hardest job is dispatching each request at its
*intended* time `t_i` (fixed before the run) with sub-millisecond skew,
even under garbage-collection (GC) pauses and operating-system
scheduler jitter, because any dispatch lateness is coordinated omission
creeping back in. It does this with a
**two-stage, nested precision design** (`internal/loadgen/loadgen.go`).
See `docs/pacer-timing.drawio` for the diagram.

**Stage 1: the pacer (one thread, spawn lead = `spawnLeadNs` = 10 ms).**
The main loop sleeps until `t_i − 10 ms`, then only *spawns* the worker
goroutine for request `i` and immediately moves on to the next. It never
does the precise wait itself. Rationale: if one thread did the exact
wait-and-dispatch inline, a hiccup while handling request `i` (GC,
scheduler) would delay `i` **and push every later request back**: the
delays would accumulate down the whole schedule. Spawning early
decouples the two stages: the 10 ms is slack that absorbs pacer-wakeup
jitter, and each worker targets its own **absolute** `t_i`, so a late
spawn shortens only that worker's runway and never cascades.

**Stage 2: the worker (precise wait = timer then spin, boundary
`spinNs` = 1.5 ms).** Each worker sleeps on a `time.Timer` until
`t_i − 1.5 ms` (cheap, yields the central processing unit (CPU), but
imprecise: `<-timer.C`
wakeup has scheduler latency), then **busy-spins** `for now() < t_i {}`
for the final 1.5 ms (precise: no timer wakeup stands between the spin
and `t_i`). `spinNs` is sized to exceed timer wakeup jitter while keeping
the CPU burn small.

The send-skew gate (SPEC §2 client-validity; table in §6 below) is
run-failing: p99 ≤ 5 ms, max ≤ 50 ms.

## 3. Anatomy of a run (the `percentes` binary, internal/run.Execute)

1. `loadgen.Run` anchors the run epoch (monotonic), fires the `OnEpoch`
   hook, and starts the pacer.
2. On the hook, the orchestrator goroutine **pre-arms** the fault to fire
   at epoch+T_inject with automatic expiry (§1 requires pre-arming: a
   black-hole partition makes the victim unreachable the moment it
   fires). The §5 decomposition probes launch, gated two-phase: a probe
   success only counts after the fault was *visible* on that path.
3. Load runs through warm-up | baseline | fault window | cooldown;
   monitors sample host CPU (1 Hz) and Go GC pauses for the §2
   client-validity gate.
4. After the last terminal event: windows are collected (baseline, guard,
   fault, a fault_survivor window over the requests served by the one
   baseline replica that is not the victim when attribution names it, plus
   degraded/recovered splits when the detector finds recovery);
   in-flight-at-fire requests are classified by outcome and by replica;
   the detector runs (two baselines, hysteresis, 27-row sensitivity
   sweep); the share gate is computed from per-request replica
   attribution; run validity is decided.
5. `report.Generate` renders the artifact pair (JSON + human-readable)
   from those artifacts alone. Exit 0 valid, 2 gate-invalid, 1 error.

`percentes-campaign` wraps this N times (per-run seed = base+i), evaluates the
§10 G1–G7 gates per run, and aggregates per-run scalars under the §7
statistics (median/mean/df-correct t-interval; heavy-tailed scalars lead
with median+range; drops are named, never imputed).

## 4. Package map

| Package | Role | Spec anchors | Key entry points | Tests |
|---|---|---|---|---|
| `internal/config` | One YAML schema drives everything; §6 pins for the Phase 0 profiles; strict decode; harness constants enforced as equalities at load, environment pins required present and compared at run time where a gate reads them | §1–§6, §8 | `LoadFile`, `Config.Validate` | mutation test per pin |
| `internal/mock` + `cmd/mockserver` | OpenAI-compatible SSE mock with analytic TTFT/ITL distributions and five scriptable fault modes | §2 "Local-first" | `New`, `Server.Start`, `/admin/faults` | per-mode behaviour tests incl. raw-TCP no-RST |
| `internal/histo` | Pinned HdrHistogram wrapper; `RecordValue()` only; lint bans correction APIs | §3 | `New`, `Record`, `Summarize` | lint (repo-wide correction-API ban); AC1 oracles via internal/ac |
| `internal/loadgen` | Open-loop generator: pre-fixed schedule, pacer+spin dispatch, SSE client, three-state classification, client-validity gates; the §2 loopback canary, one stream at a time against an in-process mock with fixed timing through the same read path | §2, §3 | `BuildSchedule`, `Run`, `StartCanary`, `SummarizeCanary` | in-package body + opt-in live-smoke units; AC1–AC2d + `-race` via internal/run; canary order-statistic oracle |
| `internal/sse` | Server-Sent Events framing for the loadgen client and naivesweep: the three line terminators, data fields joined with newlines, a leading byte-order mark tolerated, an event over the bound dropped and counted | §3 | `SplitLines`, `Events` | framing units per grammar case |
| `internal/orchestrator` | Pre-armed fault execution with armed/fire/expiry audit; injectors: mock admin, clean pod delete, node partition | §1, §2, AC3 | `Execute`, `NewMockInjector`, `NewCleanDeleteInjector`, `newNodePartitionInjector` (unexported pending Phase-1 NodeOps wiring) | AC3 + fake-ops tests |
| `internal/collect` | Windowed three-state stats, Aalen-Johansen incidence estimator, in-flight accounting, §4 sweep + modal/SD, §7 tail CIs | §3, §4, §7 | `Collect`, `EstimateIncidence`, `AccountInFlight`, `AnalyzeThresholds` | hand-computed incidence oracles, AC4/4b |
| `internal/detect` | Recovery detector (leading windows, hysteresis, two baselines, sensitivity sweep, deficit, components), decomposition scaffolding, recovery probes, /health-vs-inference calibration | §5, AC5 | `BuildSeries`, `Run`, `ProbeRecovery`, `NewPhase0Decomposition` | synthetic-series units + AC5 |
| `internal/run` | The run engine: composes everything above into one run's Artifacts | §2 | `Execute` | in-process e2e under `-race` |
| `internal/report` | JSON + human report pair from one run or one campaign; numbers read once from artifacts | §2, §3, §4, §5, §7 | `Generate`, `GenerateCampaign` | in-package renderer units + AC6 field assertions |
| `internal/redact` | Endpoint and error redaction for every published artifact and error string: userinfo, query and fragment stripped from endpoints; error text printed only for Go types that cannot carry response bytes | §6 | `URL`, `ErrorText`, `Wrap` | per-function units; report, calibration, serverstats, injector and naivesweep leak tests |
| `internal/stats` | §7 statistics: verbatim values, median, mean, df-correct t-interval, CoV/noise floor, Holm | §7 | `Summarize`, `holm` | hand-computed oracles |
| `internal/campaign` | N-run repetition engine; per-run seeds; endpoint aggregation with named drops | §5, §7, §10 | `Run` | fake-runner units |
| `internal/validity` | §10 run-validity gates G1–G7; applicable-but-unobserved ⇒ FAIL; a failed or unobserved G3/G4 strips the node-loss-representative label and the run stays valid | §10 | `Evaluate` | per-gate units |
| `internal/serverstats` | Samples each replica's Prometheus text endpoint from the run epoch, keeps the configured metric families per sample, and reduces to per-replica baseline-window means for G7 and to per-window changes per family (a gauge's mean, a counter's increase, a histogram's increase in count, sum and buckets, measured per label set between its consecutive samples and summed, a label set missing from a sample adding nothing until it returns, a fall or a changed bucket layout marked as a reset); an absent gauge or a counter read as one is an error, and so is a non-finite family value | §2, §6, §10 | `ForRun`, `Sampler.Start`/`Stop`/`FamilyErrors`/`Reduce`, `BaselineMeans`, `ReduceWindow`, `Preflight` | httptest gauge and family servers; epoch-window, reduction and reset oracles |
| `internal/hostqual` | Host qualification for the timing-coupled acceptance tests: timer wake lateness on an absolute schedule and garbage-collection (GC) pause p99 over an allocation burst, the load average recorded beside them; the dated `Allocation` sets the limits | §2, §8 | `Measure`, `Qualify`, `Qualified`, `Allocation` | per-limit and histogram units |
| `internal/calibrate` | §10 single-replica capacity calibration: coarse and fine ramps against a `Runner`, two ramps agreeing within the pinned fraction or a third deciding by median, lambda_r frozen, and the §5 reference run at 2 x lambda_r; every step is a §3 collection over its measured window with the queue-gauge series, the kept families' window reductions and the §2 receive-path report, and the trace is rewritten after every step | §10, §5, §3, §2 | `RunRamp`, `Calibrate`, `Reference`, `LoadRunner` | capacity-model fake runner; one step against the mock with a stall inside the settle |
| `cmd/percentes-calibrate` | Calibration trace pair (calibration.json, calibration.txt); exit 0/2/1; `--check` validates a config and lists its placeholders | §10 | | |
| `cmd/percentes` | One run → report pair; exit 0/2/1 | AC7 | | via reproduce.sh |
| `cmd/percentes-campaign` | N-run campaign → campaign report pair; routes `fault.variant` to its injector (mock admin / clean-delete kubectl; black-hole refused pending the Phase-1 NodeOps wiring) | §5/§7/§10 | | via campaign-e2e.sh |
| `cmd/naivesweep` | Standalone reconnaissance sweep of an OpenAI-compatible endpoint, outside the instrument: closed-loop, no client-validity gates; flags a 200 that reached `[DONE]` with no content, no refusal and no stop reason | none | | fixture cases in `main_test.go` |

Deploy/test scaffolding: `deploy/kind/` (cluster config with pinned node
image + NodePort mapping; `smoke.sh`, `reproduce.sh` = AC7,
`campaign-e2e.sh`), `deploy/mock/` (2-replica mock Deployment, no
liveness probe by design), `deploy/phase1/` (vLLM topology manifest with
PIN-AT-PHASE1 pre-registration placeholders, deliberately not deployable as-is; capture scripts for the host fingerprint, the GPU sample series and the server log), `configs/` (all runnable configs; one file drives both
cluster ConfigMap and host runner), `internal/ac/` (the §8 acceptance
suite; the mock runs as a separate process, so it never shares the
generator's Go scheduler, and the timing-coupled tests qualify the host
first through `internal/hostqual`).

## 5. Tracing any published number

Single-run report (`report.json`):

| Field | Computed in | Spec |
|---|---|---|
| `windows.*.ttft_conditional_on_completion` / `e2e_...` | `collect.Collect` → `histo.Summarize` | §3 completed-only |
| `windows.*.error_rate`, `censored_rate`, `err_classes` | `collect.Collect` | §3 first-class rates |
| `windows.*.completion_incidence` (uncrossed quantiles refuse per the §3 ceiling rule: greater-than-horizon or unattainable) | `collect.EstimateIncidence` | §3 incidence over ALL scheduled |
| `windows.*.itl_pooled` | `collect.Collect` (pooled per window) | §3 (per-request p99 forbidden) |
| `windows.*.goodput_sweep` | `collect.Collect` 3×3 grid | §4 sweep |
| `windows.*.ttft_tail_ci` / `e2e_tail_ci` | `collect.tailCIs` (exact order stats) | §7 tail policy |
| `threshold_analysis` (modal, SDs, distances) | `collect.AnalyzeThresholds` | §4 SD statement |
| `in_flight_at_fire` (+ on_victim_*) | `collect.AccountInFlight` vs injector-observed fire | §3 loss accounting |
| `detector.to_pre_fault` / `to_equilibrium` / `sensitivity` | `detect.Run` / `detect.detect` | §5 |
| `detector.equilibrium_*` (plateau, estimable, note) | `detect.Run` | §5 two baselines |
| `decomposition.segments` | `detect.NewPhase0Decomposition` + probes in `run.Execute` | §5 measured-only |
| `loadgen.gates` (skew/undispatched/CPU/GC) | `loadgen.evaluateGates` | §2 client-validity |
| `share_gate`, `victim_replica` | `run.shareGate` | §1 |
| `orchestration` (armed/fire/expiry) | `orchestrator.Execute` + injector records | §2, AC3 |
| `conditional_headline` | `report.headline` (victim-scoped) | Appendix template |

Campaign report (`campaign.json`): `campaign.per_run[*]` from
`campaign.extractScalars`, each run carrying its `receive_path`,
`server_side` and `family_errors`; `campaign.endpoints[*]` from
`stats.Summarize` with drop reasons from `campaign.summarize`;
`noise_floor_cov` only for clean_delete (§7 primary endpoint);
`validity_gates[*]` from `validity.Evaluate` per run.

## 6. The gates, and where each bites

| Gate | Pinned numbers | Where enforced |
|---|---|---|
| Config pins | the §2, §4, §5 and §10 numbers as equalities; §6 environment values as required fields | `config.Validate`: a weakened config won't load |
| Client validity (§2) | skew p99≤5ms/max≤50ms, compared in nanoseconds; zero undispatched; CPU≤70%/5s; GC p99<1ms on the upper edge of the runtime histogram bucket that holds it | `loadgen.evaluateGates` → run invalid |
| Share gate (§1/G1) | 45–55% per replica pre-fault | `run.shareGate` |
| Injection timing (AC3) | ±500 ms | `run.validity` via orchestrator records |
| G1–G7 (§10) | per SPEC | `validity.Evaluate` per run in both binaries; unobserved-but-applicable ⇒ FAIL; failed or unobserved G3/G4 strips the label, run stays valid |

G5 carries a not-applicable row: the gate does not read the nvidia-smi
fingerprint that `deploy/phase1/fingerprint.sh` captures. G7 reads `internal/serverstats`: when
`target.metrics_urls` names one Prometheus endpoint per replica, each is
sampled at the pinned cadence from the run epoch and the pinned
waiting-queue gauge (`target.queue_gauge`, `vllm:num_requests_waiting` on
vLLM, `percentes_mock_requests_waiting` on the mock) is averaged per
replica over the §3 baseline window, guard excluded; a replica with fewer
than 90 percent of the samples expected at the cadence over that window
fails coverage (§10). Unset, G7 is a not-applicable row. The kind
campaign does not set `metrics_urls`, since each pod's endpoint needs its
own address behind the single NodePort service.

The same sampler keeps every family `target.metrics_families` names on
each sample and reduces them per window and replica into the report's
`server_side` block (§2). `target.ttft_histogram` names the server-side
TTFT histogram among them; each window's `receive_path` block sets the
client-side TTFT mean against it and carries the loopback canary's event
lag against its fixed timing (p99 and max, the §2 bound on host-side
read-loop lag) with the first-token and per-gap deviations
(`internal/loadgen`), the two §2 receive-path checks, neither
run-failing. `percentes-calibrate` takes the same two settings as
`--families` and `--ttft-histogram`. `percentes`, `percentes-campaign`
and `percentes-calibrate` check every metric name and the histogram's
type against each endpoint before the load; the run report keeps the
sample series under `server_samples` and the count of failed family reads
under `family_errors`. A self-hosted request asks for the usage object
(`stream_options.include_usage`), the hosted body does not, and the
client records a completion token count from any chunk carrying one
beside its own content-event count. Per window the report carries both
counts as distributions and the §10 check over the completed requests
that carried usage; `report.txt` labels the pooled ITL inter-token when
every such request matched one content event per token, one token per
content event by construction for a mock run whose stream carried no
usage, and inter-chunk (§3) otherwise, a hosted run being inter-chunk
whatever the check says.

One consequence shows up on macOS under the CGO_ENABLED=0 builds the
make targets pin. Both binaries evaluate the §10 gates per run via
`validity.Evaluate` and fold run-invalidating failures into the exit
code; G1 and G2 are folded in by `run.Execute` itself. The client CPU
gate is unmeasured in that build (Linux measures via /proc), so such a
run exits 2. An applicable gate that goes unobserved never passes.

## 7. Fault modes and their measurement signatures

| Mode | Mock behaviour | What the instrument must show |
|---|---|---|
| `stall` | server-wide emission freeze, staggered flush on expiry | completions delayed; excess lands in p99.9/max (AC2); λ×D attributable samples (AC2b) |
| `error` | 5xx on new requests; in-flight untouched | error-rate step in the fault window; goodput dip → detector TTR |
| `throttle` | 429 on new requests; in-flight untouched | `status_429` in the fault window's error classes, absent from the baseline; error-rate step |
| `stream_abort` | RST in-flight at fire (SO_LINGER=0); admissions RST after N tokens | in-flight classified errored/reset, absent from histograms (AC4) |
| `silent_hang` | no bytes, no FIN, no RST, ever (hijacked conns); captured requests stay hung past expiry | censored at exactly 30 s, in the incidence curve as censorings, p90 beyond the horizon (AC4b) |
| `slow_reload` | 503 for a duration after process start | replica-ready probe boundary; recovery decomposition |

## 8. How to change things safely

- **Change a pinned number**: it lives in `internal/config/config.go`
  constants + `validate.go` + both reference YAMLs + a mutation test.
  Changing it in fewer than all four places fails the suite.
- **Add a metric**: compute it in `collect` (or `detect`) from the
  request records, surface it through `run.Artifacts`, render it in
  `report`; AC6 asserts report completeness, so extend its field list.
- **Add a fault mode**: `internal/mock/faults.go` engine + config enum +
  validation + a behaviour test asserting its transport-level signature
  (see the raw-TCP silent-hang test as the exemplar).
- **Swap the target for real vLLM**: nothing in loadgen/collect/detect
  changes; supply Phase 1 pins in the config, and the fault variant
  selects its injector (`run.Options.Injector`, wired in percentes-campaign;
  clean delete today, black-hole refused pending Phase-1 NodeOps, the
  run engine stays agnostic beyond timestamps, §2). Provide
  `validity.Observations` (packet capture, staleness, fingerprints).
- **Reproduce anything**: `make test` is the whole gate; each stage also
  runs alone (`test-unit`, `test-ac`, `kind-smoke`, `reproduce`,
  `campaign-e2e`).

## 9. Glossary

- **t_i / intended time**: the pre-scheduled dispatch instant; all
  latencies re-base to it (coordinated-omission correctness).
- **Send skew**: actual dispatch − intended; gated ≤5 ms p99.
- **Censored**: no terminal event by the pinned 30 s client timeout; a
  censored observation in the incidence curve, never a latency sample.
  Errors are NOT censored: they are competing terminal events (§3).
- **Goodput**: fraction of *scheduled* requests completing within the §4
  service-level objective (SLO): TTFT ≤1 s ∧ e2e ≤14 s.
- **Fire anchor**: the earlier of T_inject and the recorded actual fire
  time; the window and TTR reference point (§3).
- **Guard window**: the pinned client timeout before the fire anchor, cut
  off the end of the baseline phase because a request intended there can
  still be unresolved when the fault fires. Reported with the full metric
  set, excluded from every baseline-derived quantity (§3).
- **TTR**: time from the fire anchor to the first hysteresis-surviving
  recovery entry (leading windows), per baseline.
- **Two baselines**: pre-fault (two-replica) vs single-replica
  equilibrium (degraded-plateau estimate), different questions, never
  conflated (§5).
- **Pre-armed**: the injector knows fire time and expiry before firing;
  nothing depends on reaching the victim afterwards (§1).
- **Run-valid**: every run-failing gate passed *and was observed*.
