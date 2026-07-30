# v0.1.1 Harness Spec — Replica-Loss Resilience Characterization for Kubernetes LLM Inference
### Project: Percentes. This document is the authoritative specification for the Percentes harness.
### Status: build-ready. Supersedes v0.1.

## 0. What changed in v0.1.1 and why

v0.1.1 changes, folded into the sections below: the censoring model is built around completed-only distributions, first-class failure rates, and Kaplan-Meier completion curves; the black-hole fault is specified as one implementable mechanism — a pre-armed full-node network partition — with runtime assertions proving it produced staleness and silent drops; and every gate, tolerance, and detector parameter carries a pre-registered number.

Further changes: the TTR decomposition gets a measurability table and a two-probe split (replica-ready vs traffic-restored); the load-balancing gate covers eBPF dataplanes and asserts per-replica request share; config pins extend to client timeout, retries, chunked prefill, scheduler limits, and CUDA-graph settings; the statistics make no MDE claim for the single-stack study; real spot preemption is treated as a third regime rather than claimed as bracketed; and all Triton claims in the deferred v0.2 section are downgraded to unverified-pending-version-pin.

Framing note: the first publication is a resilience characterization of vLLM under replica loss, published under the Percentes benchmark project. The word benchmark earns itself at v0.2 when a second stack arrives.

Standing caveat: the acceptance criteria certify the instrument against a mock, not any claim about real GPU behavior. Small N on rented hardware, injected-fault-versus-reality gaps, and mock fidelity limits remain; they are scoped in the claims and named in the report.

## 1. The experiment

Replica loss is the dominant availability event for hosted LLM inference; this study measures its cost precisely.

**Question:** when one vLLM serving replica is lost under sustained load on Kubernetes, how many in-flight and queued requests fail or time out, how does the survivor degrade, and how long does recovery take, decomposed into measured sub-phases?

**Topology:** 2 GPU worker nodes, one vLLM replica each, behind one Kubernetes Service. One 8B-class dense model. Steady-state load calibrated so each replica sits inside a measured 60 to 70 percent utilization band; loss of one replica then pushes the survivor past capacity, which is the condition under study.

**Load-balancing validity gate (regime-conditional, run-failing where enforced):**
- Document the CNI and dataplane mode. Two regimes, and the gate differs between them: an eBPF dataplane such as Cilium distributes individual requests across endpoints (**per-request**), while kube-proxy in iptables or ipvs mode binds a connection to one endpoint for its lifetime (**per-connection**). The recorded `dataplane_mode` pin selects the regime; the analysis states which.
- The client does not cap connection count: pool capacity is provisioned above lambda times the pinned timeout so dispatch is never throttled by connection availability. Concurrency demand therefore sets the number of open connections. Reconnect-on-error, no request retries.
- Pre-fault assertion, from server-side request counters: each replica received 45 to 55 percent of requests over the baseline window. **Under per-request balancing this is run-failing.** Under per-connection routing the share is a binomial draw over the open-connection count — at the concurrency this protocol produces its standard deviation exceeds the band's half-width, so the share is recorded and reported but does **not** invalidate the run; a run whose balancing claim matters is executed on a per-request dataplane. Traffic reaching fewer replicas than the topology declares is run-failing in either regime.
- Phase 1 deploys a per-request (eBPF) dataplane so the band is enforced on every characterization run.
- Record which pod is killed and its share at T_inject.

**Fault variants:**
- **Clean delete:** grace=0 pod deletion. Endpoint withdrawn near-instantly, kernel RSTs in-flight connections. Labeled "abrupt replica deletion." Never called node loss.
- **Black-hole (the only node-loss-representative condition):** a pre-armed full-node network partition. All ingress and egress on the victim node is dropped, including kubelet-to-apiserver traffic, in one mechanism, which jointly produces (a) the stale-endpoint window, because the node keeps its Ready condition until node-monitor-grace-period expires (default around 40 seconds; record the cluster's actual value), and (b) silent no-RST packet drops, because nothing on the node can answer. Implementation: Chaos Mesh NetworkChaos partition targeting the node, or an iptables DROP-all rule installed by a pre-armed job. Because the node becomes unreachable the moment the fault fires, the injection MUST be pre-armed with automatic expiry (Chaos Mesh duration field, or a scheduled rule-removal job installed before the partition). The orchestrator records armed-time, fire-time, and expiry.
- **Runtime assertions for the black-hole variant (run-failing):** (i) a client-side packet capture shows zero RSTs sourced from the dead replica during the fault window; (ii) the dead pod remains present in ready EndpointSlices for an observed window of at least 20 seconds (report the measured window against the cluster's node-monitor-grace-period). A black-hole run without both assertions is reported as clean-variant-equivalent, not node-loss-representative.
- **Third regime, Phase 1 only:** real spot preemption, which includes a roughly two-minute warning and possible graceful drain, is neither variant. It is measured and reported as its own condition. The two injected variants are controllable reference conditions, not a bracket.

**Load profile:** open-loop arrivals at a pinned rate (Poisson option), fixed input length, output forced via ignore_eos with max_tokens=256, pinned prompt set with unique per-request prefixes. Phases: warm-up 60s (discarded), baseline 300s, fault at T_inject, degradation-and-recovery window with a 600s timeout, cooldown 60s.

## 2. Harness components

**Load generator (safety-critical; all changes human-reviewed).** Open-loop, coordinated-omission-correct, streaming-aware, OpenAI-compatible SSE client.
- Latency is re-based to intended dispatch time t_i and recorded via plain HdrHistogram recordValue() only. The expected-interval correction APIs are forbidden (double-correction; invalid under Poisson).
- Connection and worker capacity provisioned above lambda times worst-case latency so the generator never throttles in sympathy with the system.
- Monotonic clock only.
- No request retries. Reconnect-on-error applies to subsequent requests; a failed request stays failed.
- **Client-validity gate, pinned numbers, run-failing:** send skew (actual minus intended dispatch) p99 at most 5 ms and max at most 50 ms; zero scheduled-but-never-dispatched requests; client CPU at most 70 percent sustained over any 5 s window; if implemented in Go, GC pause p99 under 1 ms during measurement windows, reported.

**Chaos orchestrator.** Fires the configured variant at T_inject; records armed/fire/expiry timestamps; pluggable injectors (mock fault modes locally; clean delete, node partition, or spot capture in Phase 1). The harness is agnostic beyond timestamps.

**Metrics collector.** Client-side stream is authoritative for latency, errors, and censoring. Server-side vLLM Prometheus metrics are explanatory and provide per-replica request counters for the share gate, plus batching and cache occupancy. Records client-to-service RTT and generator placement; server-side TTFT histograms reported alongside client-side.

**Recovery detector.** Hysteresis-based, two baselines, decomposed. Section 5.

**Report generator.** Full metric set as JSON plus human-readable report from one config, including KM curves and the conditional headline (appendix). Distributional metrics come from merged HdrHistograms queried once, never averaged percentiles.

**Local-first.** Everything runs in Phase 0 on kind or k3s with a mock inference server speaking the same SSE API, with configurable TTFT and per-token latency and fault modes: stall, error, stream-abort, slow-reload-on-reschedule, and silent-hang (no RST). Zero GPU until Phase 1.

## 3. Metrics and the censoring treatment (normative)

Every scheduled request is a sample and ends in exactly one state:
- **Completed:** latency = completion_time minus t_i. Enters the latency histograms.
- **Errored:** explicit failure (5xx, reset, malformed stream) at failure_time. Counted in the error rate for its window. NEVER enters latency histograms.
- **Censored:** no terminal event by the pinned client timeout (30 s) or run end. Counted in the censored rate with its observation time. NEVER enters latency histograms.

Reporting per window:
- **Completed-only distributions:** TTFT and end-to-end percentiles from merged HdrHistograms over completed requests, always labeled "conditional on completion."
- **Failure rates as first-class headline metrics:** error rate and censored rate per window, alongside in-flight loss accounting (requests active on the killed replica at T_inject, classified by outcome).
- **Kaplan-Meier completion curves** over ALL scheduled requests in the window: completions are events at their latency; errored and censored requests are censored observations at their observed times. Report the curve for baseline, fault, and recovery windows. A KM quantile q is reported only where the curve actually crosses q within the timeout horizon; otherwise report "p_q greater than 30 s."
- **Conditional-percentile rule:** any window with error-plus-censored fraction above 5 percent must present the KM curve alongside any completed-only percentiles, with the caveat explicit.

Also normative: intended dispatch times fixed in advance; ITL as pooled per-window histograms (per-request p99 forbidden; per-request max named as such); one pinned HdrHistogram configuration across all runs and windows with highestTrackableValue at least the run timeout, enabling lossless merge; windows aligned to pre-fault, during-fault, post-fault, never straddling T_inject; throughput as completions per second per window; goodput as the fraction of a window's scheduled requests that complete within the §4 SLO, also reported as SLO-meeting completions per second; loss counts reported as a function of the pinned 30 s timeout.

## 4. SLO (pre-registered)

A request meets SLO iff TTFT at most 1000 ms, end-to-end at most 14 s (1000 ms plus 256 tokens at a 20 tokens-per-second floor), and completion without error. Fixed before any fault data; identical across variants. Baseline goodput must be near 100 percent or the load calibration is wrong and is redone. The report includes a goodput-versus-threshold sweep (TTFT threshold at 800, 1000, 1500 ms; e2e at 12, 14, 18 s) and states how many baseline standard deviations the modal during-fault latency sits from each threshold.

## 5. Recovery (two baselines, hysteresis, measured decomposition)

**Two baselines, both reported:** time to the stable post-fault single-replica equilibrium, and time to the two-replica pre-fault baseline. They answer different questions and are never conflated.

**Detector, pre-registered numbers:** goodput over a sliding window R=10 s; recovery entry at X=90 percent of the applicable baseline goodput, exit (re-degradation) below 85 percent; hold H=30 s of consecutive windows above entry. TTR = first entry that survives the hold, minus T_inject. Sensitivity table over X in {85, 90, 95}, R in {5, 10, 20}, H in {15, 30, 60}; if TTR ordering across variants is not stable over the sweep, no ordering claim is made. Companion metric: integrated goodput deficit (area between baseline and observed goodput from T_inject to recovery), less threshold-fragile than any crossing time.

**Per-component recovery:** report recovery separately for TTFT-SLO, e2e-SLO, and error rate, plus a backlog-drain time where a failed-request backlog exists.

**Decomposition with measurability stated (only measured boundaries are claimed):**
- Reschedule (kill to new pod scheduled): Kubernetes events and pod timestamps. Measured via API.
- Container start: containerStatuses startedAt. Measured via API.
- Weight load: vLLM log lines, scraped with patterns pinned to the deployed version. Log-derived; if the pinned version does not emit it, the boundary is reported N/A, never inferred.
- CUDA-graph capture: vLLM log lines, same rule. Log-derived or N/A.
- **Replica-ready:** first successful inference against the pod IP directly. Harness probe.
- **Traffic-restored:** first successful inference via the Service. Harness probe. The gap between these two is routing propagation and is reported as its own segment.
- Goodput restored: from the client stream per the detector.
- Phase 1 setup includes a one-off calibration comparing /health 200 timing against direct first-inference success on the pinned vLLM version; the relationship is documented, not assumed.

**Repetition:** N=5 runs per (variant, config). All five per-run values are published verbatim alongside the statistics.

## 6. Configuration control (enforced, verified, pinned)

- KV-cache budget pinned in absolute gigabytes; continuous batching verified active from server metrics; prefix caching verified OFF via server metrics for the pinned version (defaults are version-dependent; verify, do not assume); unique per-request prefixes regardless.
- max_tokens=256 with ignore_eos; tokens-lost and the e2e SLO defined against that budget.
- Pinned and recorded: vLLM version and image digest, model and revision, quantization, max-num-seqs and scheduler settings, chunked-prefill setting, CUDA-graph enablement, GPU SKU, driver, CUDA, cuDNN, NCCL, Kubernetes version, CNI and dataplane mode, kube-proxy mode, node-monitor-grace-period, client HTTP timeout (30 s), retries (zero), readiness probe configuration, storage medium for weights, and GPU clock and power policy asserted equal across runs (nvidia-smi fingerprint per replica per run).
- Client placement pinned: dedicated non-spot node, same zone and subnet, RTT recorded in the environment table.

## 7. Statistics (single-stack study)

- **Pre-registered primary endpoint:** TTR to single-replica equilibrium under the clean-delete variant. Everything else is secondary or exploratory and labeled so; Holm correction where several secondaries are formally compared.
- Run-level scalars: report all five values, the median, the mean, and a t-interval (t=2.776, df=4). For plausibly heavy-tailed scalars (the TTRs), the median and the min-max range lead; the t-interval is reported with a normality caveat rather than as the headline. Bootstrap at N=5 is forbidden.
- No MDE claim is made for the single-stack study; there is no comparison to power. The v0.1.1 run-to-run coefficient of variation becomes the measured noise floor that v0.2's comparison design and its pre-registered two-sample MDE will be built on.
- Tail policy: p95 and p99 with order-statistic confidence intervals where the completed-sample budget permits; p99.9 and max are descriptive-only unless a long steady-state run is explicitly sized for them with a binomial validity gate. No estimated quantiles from short fault windows.

## 8. Acceptance criteria (Phase 0, against the mock; numbers pinned)

Reference conditions for the AC suite: lambda=20 rps, stall duration D=10 s where used.

- **AC1 measurement correctness:** reported p50/p95/p99 within plus-minus 2 percent or 1 ms, whichever is greater, of a known injected distribution.
- **AC2 CO plumbing:** a known mid-run stall of D is reflected in p99.9 and max within 5 percent of D.
- **AC2b tail-sample count:** recorded samples attributable to the stall within plus-minus 10 percent of D times lambda.
- **AC2c zero un-dispatched:** any scheduled-but-never-dispatched request fails the run; send-skew gate numbers per section 2 enforced and reported.
- **AC2d client validity:** under injected client CPU pressure (stress on 80 percent of cores), the gate fires on the pinned thresholds rather than silently degrading.
- **AC3 injection timing:** fault fires within plus-minus 500 ms of T_inject; armed/fire/expiry timestamps recorded.
- **AC4 loss accounting:** in-flight requests on a killed mock replica are classified (errored) and appear in the failure rates, absent from latency histograms.
- **AC4b censoring accounting:** in silent-hang mode, affected requests are censored at exactly the 30 s pinned timeout, appear in the censored rate and the KM curve, and are absent from latency histograms.
- **AC5 recovery detection:** scripted mock recovery yields TTR within plus-minus R of the script; both baselines reported distinctly; hysteresis prevents oscillation-induced early recovery in a scripted flapping scenario; non-recovery past the 600 s timeout reported as such.
- **AC6 reporting:** JSON plus human-readable report from one config, including KM curves, failure rates, the sensitivity table, and the conditional headline.
- **AC7 harness reproducibility:** one-command reproduce from a clean checkout against the local cluster.

Caveat, printed in the AC output itself: passing AC1 through AC7 certifies the instrument, not any real-GPU claim.

## 9. Build order (Phase 0, zero GPU; each module gated on its ACs)

1. Scaffold and config schema carrying the full section 6 pin list.
2. Mock inference server with all fault modes including silent-hang and slow-reload.
3. Load generator. Gated on AC1, AC2, AC2b, AC2c, AC2d. The scheduling, re-basing, and recording design requires independent human review before merge.
4. Chaos orchestrator with pre-armed expiry semantics. AC3.
5. Metrics collector with the three-state outcome model and KM estimator. AC4, AC4b. The censoring implementation requires independent human review before merge.
6. Recovery detector with hysteresis, two baselines, decomposition scaffolding. AC5. Independent human review before merge.
7. Report generator. AC6.
8. End-to-end one-command run. AC7.

Language: Go or Rust for the load generator; the choice is documented with rationale; if Go, the GC-pause budget in section 2 is measured and asserted. Python is acceptable for orchestration, reconciliation, and reporting. HdrHistogram via recordValue() only.

## 10. Phase 1 (real GPU) and run-validity gates

Swap the mock for vLLM, two replicas across two GPU nodes, same model and output budget. Calibrate lambda to the 60-70 percent utilization band. Run both variants, N=5 each. Runs are short and deliberately inexpensive to reproduce.

Deploy a per-request (eBPF) dataplane, per §1, so the share gate is enforced rather than descriptive.

**Run-validity gates, all run-failing:** G1 per-replica share 45-55 percent pre-fault (enforced under per-request balancing; descriptive under per-connection routing, §1); G2 client-validity gate clean; G3 (black-hole only) zero RSTs from the dead replica in the capture; G4 (black-hole only) observed endpoint-staleness window at least 20 s; G5 GPU clock and power fingerprints equal across replicas and runs; G6 baseline goodput near 100 percent.

**Proxy validation, honestly framed:** pre-register the equivalence quantities (in-flight loss fraction, TTR, survivor p95) and a tolerance; collect real spot-preemption events opportunistically (running on spot makes them free) and report their distribution against the two injected variants as a third regime. A single real event confirms the code path, nothing more, and the report says so.

## 11. v0.2 (deferred cross-stack comparison)

The v0.2 study compares vLLM against a second serving stack (Triton) on the same harness, gates, and pre-registered protocol. All Triton capability claims (backend behavior, native TTFT/ITL exposure, dynamic-batcher status, streaming order) are UNVERIFIED pending a pinned Triton version; recent Triton releases may expose latency metrics, so nothing in the v0.2 protocol may be asserted from this document — every capability claim is re-verified against the pinned versions at build time. The v0.2 comparison additionally requires the measured noise floor from v0.1.1 and a pre-registered two-sample MDE before any winner language.

## Appendix: conditional headline template (censoring-aware)

"Under abrupt loss of one vLLM replica at [measured] percent utilization, [A] percent of in-flight requests failed immediately (clean deletion) versus [B] percent failing and [C] percent timing out at 30 s (black-hole node loss); survivor TTFT (conditional on completion) degraded from [baseline] to [peak], with completion probability within 1 s falling to [KM figure]; recovery to single-replica equilibrium took [T_eq] (median of 5 runs, range [lo]-[hi]), decomposed as [measured segments], with [dominant segment] dominating. Methodology, raw per-run data, and the reproducible harness: [link]."
