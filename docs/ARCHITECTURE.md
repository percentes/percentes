# Percentes architecture — how to understand and unravel what's here

This document maps the whole system: what each piece does, why it exists
(with the SPEC.md clause it serves), how data flows from a scheduled
request to a number in a report, and how to take any published number
and trace it back to the line of code that produced it. Read it top to
bottom once; after that, the trace tables in §5 are the working index.

SPEC.md is authoritative everywhere.

## 1. What this is

Percentes measures LLM-inference reliability under load and failure.
Replica loss is the Phase 0/1 fault class: what happens when a
Kubernetes-served LLM inference service loses a replica under sustained
load — how many in-flight and
queued requests fail or time out, how the survivor degrades, and how long
recovery takes. Phase 0 (complete) builds and certifies the *instrument*
against a mock inference server on a local kind cluster — passing the
acceptance suite certifies the measurement machinery, not any real-GPU
claim. The Phase 1 groundwork (complete, GPU-untouched) adds everything
for the real experiment that can be verified without hardware.

The core methodological commitments, which shape almost every design
decision you'll see:

- **Open loop**: requests are dispatched on a schedule fixed before the
  run. The generator never slows down because the system did — a stalled
  system must not be allowed to hide its own stall (coordinated
  omission, SPEC §2).
- **Three-state outcomes** (§3): every scheduled request ends exactly one
  of completed / errored / censored. Only completions enter latency
  histograms; failures are first-class rates; censored requests (no
  terminal event by the pinned 30 s timeout) enter Kaplan-Meier curves,
  never latency percentiles.
- **Pre-registered numbers**: every gate, tolerance, and detector
  parameter is pinned in SPEC.md, enforced at config-load time, and a
  config that weakens one refuses to load.
- **Honesty over coverage**: anything unmeasured is reported as
  unmeasured-and-failing (gates) or N/A-with-reason (segments,
  equilibrium), never inferred or silently passed.

## 2. Lifecycle of one request

```
 schedule.go            sse_client.go                    mock / vLLM
 ───────────            ─────────────                    ───────────
 t_i fixed before run → worker wakes at t_i (timer+spin) → POST /v1/chat/completions (SSE)
                        dispatch recorded (skew = dispatch−t_i)
                        first content chunk → FirstTokNs          TTFT = FirstTok − t_i
                        per-token gaps → ITLsUs[]                 (re-based to INTENDED time)
                        terminal event:
                          data:[DONE]      → completed, DoneNs    e2e = Done − t_i
                          5xx/RST/bad SSE  → errored (+class)
                          ctx deadline 30s → censored
```

Downstream, `collect.Collect` assigns the request to the window of its
*intended* time (windows never straddle T_inject, §3), records completed
latencies into the pinned HdrHistogram configuration via `recordValue()`
only, adds every request to that window's KM curve, and accumulates
failure rates, goodput, the §4 threshold sweep, and §7 tail CIs.
`detect.BuildSeries` buckets the same requests at 1 Hz for the recovery
detector. Nothing is computed twice from different sources: report
numbers come from these artifacts and only these.

## 2.1 The pacer timing model (how dispatch stays a pure function of the clock)

The load generator's hardest job is dispatching each request at its
*intended* time `t_i` (fixed before the run) with sub-millisecond skew,
even under GC pauses and OS scheduler jitter — because any dispatch
lateness is coordinated omission creeping back in. It does this with a
**two-stage, nested precision design** (`internal/loadgen/loadgen.go`).
See `docs/pacer-timing.drawio` for the diagram.

**Stage 1 — the pacer (one thread, spawn lead = `spawnLeadNs` = 10 ms).**
The main loop sleeps until `t_i − 10 ms`, then only *spawns* the worker
goroutine for request `i` and immediately moves on to the next. It never
does the precise wait itself. Rationale: if one thread did the exact
wait-and-dispatch inline, a hiccup while handling request `i` (GC,
scheduler) would delay `i` **and shove every later request back** — the
delays would accumulate down the whole schedule. Spawning early
decouples the two stages: the 10 ms is slack that absorbs pacer-wakeup
jitter, and each worker targets its own **absolute** `t_i`, so a late
spawn shortens only that worker's runway and never cascades.

**Stage 2 — the worker (precise wait = timer then spin, boundary
`spinNs` = 1.5 ms).** Each worker sleeps on a `time.Timer` until
`t_i − 1.5 ms` (cheap — yields the CPU, but imprecise: `<-timer.C`
wakeup has scheduler latency), then **busy-spins** `for now() < t_i {}`
for the final 1.5 ms (precise — never yields, so zero wakeup latency;
costs ~3% of one core at 20 rps). The timer covers the bulk cheaply; the
spin buys the accuracy the timer can't. `spinNs` is sized to comfortably
exceed timer wakeup jitter while keeping the CPU burn small.

The two constants are two nested guards: `spawnLeadNs` (10 ms) decouples
the *pacer* from the *worker* so pacer jitter can't cascade; `spinNs`
(1.5 ms) decouples the *timer's imprecision* from *dispatch* so scheduler
jitter can't leak into send-skew. The send-skew gate (§6) is the check
that the whole mechanism worked: p99 ≤ 5 ms, max ≤ 50 ms, run-failing.

## 3. Anatomy of a run (`percentes run`, internal/run.Execute)

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
4. After the last terminal event: windows are collected (baseline, fault,
   plus degraded/recovered splits when the detector finds recovery);
   in-flight-at-fire requests are classified by outcome and by replica;
   the detector runs (two baselines, hysteresis, 27-row sensitivity
   sweep); the share gate is computed from per-request replica
   attribution; run validity is decided.
5. `report.Generate` renders the artifact pair (JSON + human-readable)
   from those artifacts alone. Exit 0 valid, 2 gate-invalid, 1 error.

`percentes-campaign` wraps this N times (per-run seed = base+i), evaluates the
§10 G1–G6 gates per run, and aggregates per-run scalars under the §7
statistics (median/mean/df-correct t-interval; heavy-tailed scalars lead
with median+range; drops are named, never imputed).

## 4. Package map

| Package | Role | Spec anchors | Key entry points | Tests |
|---|---|---|---|---|
| `internal/config` | One YAML schema drives everything; full §6 pin list; strict decode; every pre-registered number enforced as an equality at load | §1–§6, §8 | `LoadFile`, `Config.Validate` | mutation test per pin |
| `internal/mock` + `cmd/mockserver` | OpenAI-compatible SSE mock with analytic TTFT/ITL distributions and five scriptable fault modes | §2 "Local-first" | `New`, `Server.Start`, `/admin/faults` | per-mode behavior tests incl. raw-TCP no-RST |
| `internal/histo` | Pinned HdrHistogram wrapper; `recordValue()` only; lint bans correction APIs | §3 | `New`, `Record`, `Summarize` | lint + oracles |
| `internal/loadgen` | Open-loop generator: pre-fixed schedule, pacer+spin dispatch, SSE client, three-state classification, client-validity gates | §2, §3 | `BuildSchedule`, `Run` | AC1–AC2d + `-race` via internal/run |
| `internal/orchestrator` | Pre-armed fault execution with armed/fire/expiry audit; injectors: mock admin, clean pod delete, node partition | §1, §2, AC3 | `Execute`, `NewMockInjector`, `NewCleanDeleteInjector`, `NewNodePartitionInjector` | AC3 + fake-ops tests |
| `internal/collect` | Windowed three-state stats, KM estimator, in-flight accounting, §4 sweep + modal/SD, §7 tail CIs | §3, §4, §7 | `Collect`, `EstimateKM`, `AccountInFlight`, `AnalyzeThresholds` | hand-computed KM oracles, AC4/4b |
| `internal/detect` | Recovery detector (leading windows, hysteresis, two baselines, sensitivity sweep, deficit, components), decomposition scaffolding, recovery probes, /health-vs-inference calibration | §5, AC5 | `BuildSeries`, `Run`, `ProbeRecovery`, `NewPhase0Decomposition` | synthetic-series units + AC5 |
| `internal/run` | The run engine: composes everything above into one run's Artifacts | §2 | `Execute` | in-process e2e under `-race` |
| `internal/report` | JSON + human report pair from one run or one campaign; numbers read once from artifacts | §2, §3, §4, §5, §7 | `Generate`, `GenerateCampaign` | AC6 field assertions |
| `internal/stats` | §7 statistics: verbatim values, median, mean, df-correct t-interval, CoV/noise floor, Holm | §7 | `Summarize`, `Holm` | hand-computed oracles |
| `internal/campaign` | N-run repetition engine; per-run seeds; endpoint aggregation with named drops | §5, §7, §10 | `Run` | fake-runner units |
| `internal/validity` | §10 run-validity gates G1–G6; applicable-but-unobserved ⇒ FAIL | §10 | `Evaluate` | per-gate units |
| `cmd/percentes` | One run → report pair; exit 0/2/1 | AC7 | | via reproduce.sh |
| `cmd/percentes-campaign` | N-run campaign → campaign report pair; routes `fault.variant` to its injector (mock admin / clean-delete kubectl / node-partition) | §5/§7/§10 | | via campaign-e2e.sh |

Deploy/test scaffolding: `deploy/kind/` (cluster config with pinned node
image + NodePort mapping; `smoke.sh`, `reproduce.sh` = AC7,
`campaign-e2e.sh`), `deploy/mock/` (2-replica mock Deployment, no
liveness probe by design), `deploy/phase1/` (vLLM manifests +
bring-up notes), `configs/` (all runnable configs; one file drives both
cluster ConfigMap and host runner), `internal/ac/` (the §8 acceptance
suite; mock runs as a separate process per §6 placement pinning).

## 5. Tracing any published number

Single-run report (`report.json`):

| Field | Computed in | Spec |
|---|---|---|
| `windows.*.ttft_conditional_on_completion` / `e2e_...` | `collect.Collect` → `histo.Summarize` | §3 completed-only |
| `windows.*.error_rate`, `censored_rate`, `err_classes` | `collect.Collect` | §3 first-class rates |
| `windows.*.km_curve` (+ "p_q > 30s" quantiles) | `collect.EstimateKM` | §3 KM over ALL scheduled |
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
`campaign.extractScalars`; `campaign.endpoints[*]` from
`stats.Summarize` with drop reasons from `campaign.summarize`;
`noise_floor_cov` only for clean_delete (§7 primary endpoint);
`validity_gates[*]` from `validity.Evaluate` per run.

## 6. The gates, and where each bites

| Gate | Pinned numbers | Where enforced |
|---|---|---|
| Config pins | every §6/§4/§5 number | `config.Validate` — a weakened config won't load |
| Client validity (§2) | skew p99≤5ms/max≤50ms; zero undispatched; CPU≤70%/5s; GC p99<1ms | `loadgen.evaluateGates` → run invalid |
| Share gate (§1/G1) | 45–55% per replica pre-fault | `run.shareGate` |
| Injection timing (AC3) | ±500 ms | `run.validity` via orchestrator records |
| G1–G6 (§10) | per SPEC | `validity.Evaluate` per campaign run; unobserved-but-applicable ⇒ FAIL |

Notable consequence you will see in every local report, but from two
different gates depending on which binary you run. A single `percentes`
run is judged by `run.validity` (client-validity gate + share gate +
injection timing only — it does not evaluate G1–G6), so on macOS it
exits 2 because the CPU gate is unmeasured (CGO-free build; Linux
measures via /proc). A `percentes-campaign` run additionally evaluates G1–G6
per run via `validity.Evaluate`, so it exits 2 for the CPU gate *and*
because G5 has no GPU fingerprint pre-hardware. Either way an
uncertified gate never silently passes — that is deliberate.

## 7. Fault modes → measurement signatures

| Mode | Mock behavior | What the instrument must show |
|---|---|---|
| `stall` | server-wide emission freeze, staggered flush on expiry | completions delayed; excess lands in p99.9/max (AC2); λ×D attributable samples (AC2b) |
| `error` | 5xx on new requests; in-flight untouched | error-rate step in the fault window; goodput dip → detector TTR |
| `stream_abort` | RST in-flight at fire (SO_LINGER=0); admissions RST after N tokens | in-flight classified errored/reset, absent from histograms (AC4) |
| `silent_hang` | no bytes, no FIN, no RST, ever (hijacked conns); captured requests stay hung past expiry | censored at exactly 30 s, in KM as censorings, "p_90 > 30s" (AC4b) |
| `slow_reload` | 503 for a duration after process start | replica-ready probe boundary; recovery decomposition |

## 8. How to unravel or change things safely

- **Change a pinned number**: it lives in `internal/config/config.go`
  constants + `validate.go` + both reference YAMLs + a mutation test.
  Changing it in fewer than all four places fails the suite — by design.
- **Add a metric**: compute it in `collect` (or `detect`) from the
  request records, surface it through `run.Artifacts`, render it in
  `report`; AC6 asserts report completeness, so extend its field list.
- **Add a fault mode**: `internal/mock/faults.go` engine + config enum +
  validation + a behavior test asserting its transport-level signature
  (see the raw-TCP silent-hang test as the exemplar).
- **Swap the target for real vLLM**: nothing in loadgen/collect/detect
  changes; supply Phase 1 pins in the config, and the fault variant
  selects its injector (`run.Options.Injector`, wired in percentes-campaign —
  the run engine stays agnostic beyond timestamps, §2). Provide
  `validity.Observations` (packet capture, staleness, fingerprints).
- **Reproduce anything**: `make test` is the whole gate; each stage also
  runs alone (`test-unit`, `test-ac`, `kind-smoke`, `reproduce`,
  `campaign-e2e`).

## 9. Glossary

- **t_i / intended time**: the pre-scheduled dispatch instant; all
  latencies re-base to it (coordinated-omission correctness).
- **Send skew**: actual dispatch − intended; gated ≤5 ms p99.
- **Censored**: no terminal event by the pinned 30 s client timeout; a
  KM censoring observation, never a latency sample.
- **Goodput**: fraction of *scheduled* requests completing within the §4
  SLO (TTFT ≤1 s ∧ e2e ≤14 s).
- **TTR**: time from actual fire to the first hysteresis-surviving
  recovery entry (leading windows), per baseline.
- **Two baselines**: pre-fault (two-replica) vs single-replica
  equilibrium (degraded-plateau estimate) — different questions, never
  conflated (§5).
- **Pre-armed**: the injector knows fire time and expiry before firing;
  nothing depends on reaching the victim afterwards (§1).
- **Run-valid**: every run-failing gate passed *and was observed*.
