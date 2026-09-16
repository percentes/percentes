# Changes to the rules in SPEC.md

Every change to a measurement or reporting rule since first publication, newest first. Entries dated before 15 September 2026 were compiled on that date from the commit history of SPEC.md, which is the primary record; the commits are listed in the private register and can be re-derived with `git log -p -- SPEC.md`. Each entry states the rule as that change left it, so an entry a later change supersedes is history and the newest entry for a rule is the one in force. Wording, structure, status lines and code are in the git log.

## 2026-09-15

- §10: when the two calibration ramps agree within 10 percent of the larger value, lambda_max is the lower of the two.
- §10: a ramp step's waiting-queue mean is taken only when at least 90 percent of the samples expected at the pinned cadence landed inside its measured portion; a step short of that is not judged and the procedure stops as an execution error recorded in the trace. G7 applies the same 90 percent threshold per replica over the baseline window, and a replica below it fails coverage.
- §5: the Phase 1 single-replica reference run is offered 2 lambda_r over the §1 warm-up and baseline durations.
- §3: the pinned HdrHistogram configuration is lowest discernible value 1 microsecond, highest trackable value at least the 600 s run timeout, 3 significant figures.

## 2026-09-14

- §6: the G7 gauge name (`vllm:num_requests_waiting` on vLLM, re-verified against the pinned version) and the 1 s sample cadence from the run epoch are pinned and recorded.
- §10: G7 is evaluated from the pinned gauge when `target.metrics_urls` names one Prometheus endpoint per replica; a replica with no baseline sample fails coverage; unset, G7 reports not applicable.

## 2026-09-13

- §2: the Phase 0 mock offers a throttle fault mode (429 on new requests) beside stall, error, stream-abort, slow-reload-on-reschedule and silent-hang.

## 2026-09-06

- §3: an errored request carries exactly one of six classes by stage: `reset` or `connect` before a status; `status_429` or `status_other` on a delivered non-200 status (redirects are not followed, so a 3xx is a delivered status and takes `status_other`); `empty_stream`, `reset` or `malformed_stream` on a 200 stream. Where tests overlap, precedence is deadline (censored), then reset, then the residual class of the stage. A delivered status is terminal even if the per-request deadline fires after it; failure time is the status arrival or the failing read; the error body is not read and the connection is closed.
- §3: dispatched requests drain to their own deadline, so run end censors only an aborted run; a timeout after content events is censored and partial content never enters a latency histogram.
- §6: the separate hosted protocol covers at minimum request rate, run duration, prompt set, endpoint and model selection, the temporal sampling frame, the treatment of HTTP 429 responses and any provider-specific admission-control signals beyond their §3 classification, and any hosted SLO.

## 2026-08-25

- §10: G5 requires the nvidia-smi fingerprint collector and reports not applicable until that collector exists in Phase 1.

## 2026-08-24

- §3: a 200 stream that reaches the [DONE] terminator with no prior content event is errored (class `empty_stream`), counted in the window error rate and excluded from latency histograms; any other termination before [DONE] is `reset` or `malformed_stream`.

## 2026-08-21

- §1: Phase 1 victim attribution uses the pinned layer-7 proxy's upstream-endpoint response header and its request-ID-keyed access log; server-side per-replica counters serve the share gate only.
- §1: the guard window runs from 30 s before the fire anchor to T_inject, so an early fire lengthens it.
- §3: in-flight loss accounting counts the requests active on the killed replica at fire, classified by outcome.
- §3: a quantile crossing is tested in floating point with absolute tolerance 1e-9; the ceiling is final completion incidence plus the event-free survival remaining where the curve ends.
- §4: modal latency is the densest-bin centre of fault-window completions (bins 1 percent of the window median, 1 ms floor); baseline deviation is the n-minus-1 sample standard deviation; zero deviation reports signed infinity; fewer than two baseline completions or zero fault-window completions reports insufficient.
- §6: the pinned proxy configuration names the upstream-endpoint response header and the client request-ID header; the client pool carries an idle-provisioning floor of 4 connections per replica; the configuration `schema_version` is pinned at 1.
- §6: against a hosted target the completion token count is reported as a distribution where the response carries one and otherwise as not verifiable; the client-side SSE content event count is reported as a distribution either way.
- §7: p95 and p99 intervals are two-sided 95 percent distribution-free order-statistic intervals with normal-approximation binomial ranks, reported only where both ranks fall inside the completed sample.
- §10: if the 2 rps coarse step fails, calibration is invalid and no lambda_max is recorded; a third ramp decides by median when the two values differ by more than 10 percent of the larger; per-replica GPU clock and throttle-reason counters are recorded over the fault window beside G5; the proxy-validation tolerance, minimum event count and decision rule are pinned, dated and published here before any spot-preemption comparison is published.
- §11: a cross-stack comparison requires the single-stack noise floor, a pre-registered two-sample minimum detectable effect and pointwise interval bands on the curves before any winner language.

## 2026-08-20

- Appendix: the headline template reports survivor TTFT degradation as the baseline-window p50 against the fault-window p50, conditional on completion; it states 65 percent of measured single-replica capacity, calls black-hole a network partition, marks the time to single-replica equilibrium as clean delete, and adds a slot for the time to the two-replica pre-fault baseline.
- §1: lambda_r is frozen at 0.65 of measured single-replica capacity and utilization is that ratio; occupancy gauges do not define the band.
- §1: per-request balancing means a request-aware layer-7 proxy; kube-proxy and Cilium's eBPF replacement are per-connection, and the regime is per-connection unless the pin documents layer 7. Phase 1 runs behind a pinned layer-7 proxy with proxy-level retries disabled.
- §1: characterization runs use open-loop Poisson arrivals at the pinned rate; deterministic arrivals are accepted for diagnostics.
- §1: Phase 0 victim attribution comes from the mock-only `X-Percentes-Replica` first-token header, so a request cut before its first token carries none (Phase 1 attribution as revised on 21 August).
- §1, §6: the black-hole partition lasts a pinned 120 s set in configuration and expires automatically; a black-hole run fails when the cluster's node-monitor-grace-period is at or above 120 s; black-hole represents node loss only during the fault window and its recovery is labelled partition heal.
- §1, §10: G3 (black-hole only) requires zero errored outcomes among victim-attributed in-flight requests with every non-completer censored at 30 s; packet-capture RST counts are reported and no gate uses them. G4 requires a stale ready EndpointSlice window of at least 20 s and at least one post-T_inject connection or request observed routed to the stale endpoint inside it. G3 or G4 failing or unobserved strips the node-loss label while the run stays valid; any other applicable gate failure invalidates the run.
- §1, §3, §5: the final 30 s before the fire anchor is a guard window, reported with the full per-window metric set and excluded from every baseline-derived quantity; window membership follows intended dispatch time, so a fault-terminated request intended in the guard window counts there and stays out of the fault window's error rate.
- §3, §5: the fire anchor is the earlier of T_inject and the recorded actual fire time; TTR and the integrated goodput deficit are measured from it.
- §3: an uncrossed quantile is reported as greater than the horizon, with the ceiling, when final incidence plus outstanding censored mass reaches q, and otherwise as unattainable; censoring arises only from the pinned timeout or run end, the generator drains every scheduled request, and a run-end censoring inside the horizon marks an aborted run that is not published.
- §3: completion-incidence curves and crossing times are published as point estimates with n, and interval bands are required before any cross-provider or cross-stack comparison; ITL histograms are gaps between SSE content events, labelled inter-token only where a one-token-per-event invariant is verified for the pinned server and otherwise inter-chunk; completions up to 50 ms past the 30 s horizon stay on the curve and quantiles are claimed only inside it; an inter-event gap below 1 microsecond is recorded as 1 microsecond.
- §4, §10: baseline goodput over the pre-fault window must be at least 0.99; below that the calibration is redone and G6 fails the run.
- §5: single-replica equilibrium is ratio-of-sums SLO goodput from the fire anchor plus R (10 s) to the earlier of pre-fault recovery entry or 600 s; a window shorter than R, or with zero goodput, is not estimable and TTR is not applicable. Phase 1 adds one single-replica no-fault reference run per (model, config) at identical load, prompts and timeout, published beside the plateau estimates and used by no gate. Under black-hole the reschedule, container-start, weight-load and CUDA-graph segments are not applicable, and replica-ready and traffic-restored are partition-heal segments measured from partition expiry.
- §2: under Go, GC pause p99 under 1 ms during measurement windows joins the run-failing client-validity gate; connection and worker capacity exceed lambda times the 30 s timeout; client-versus-server TTFT divergence and a loopback canary are reported per window and neither is run-failing; per replica and window the collector records the vLLM waiting and running gauges, KV-cache occupancy and GPU busy percent as context, reported not applicable, never inferred, where the pinned version does not emit a gauge.
- §6: pinned and recorded: partition duration, pod toleration seconds, Deployment update strategy, PodDisruptionBudget presence, cluster-autoscaler status (absent or disabled) and per-run pod eviction. Self-hosted and mock targets pin `ignore_eos` true; hosted targets pin it false with max_tokens 256 as a ceiling; a mismatched value refuses to load. Against a hosted target the listed server-side pins report as not verifiable, completion tokens as a distribution, tokens-lost does not transfer, no hosted SLO is defined, only G2 and G6 are evaluable (G6 as completion within the 30 s timeout, published marked failed below 0.99), and the run is not a §1 run. No hosted measurement is published under this document; a pin-and-refuse hosted protocol is published before any provider data is collected.
- §6, §10: lambda_max is measured by pod IP before characterization: double from 2 rps, then steps of 10 percent; 30 s settle and 120 s measured per step; a step passes at goodput at least 99 percent, queue mean at most 1.0 and a clean client gate; the procedure runs twice with a median-deciding third ramp on more than 10 percent disagreement; the full trace is published; lambda_r is frozen at 0.65 lambda_max until a §6 pin changes.
- §10: G7 fails a run whose per-replica mean waiting-queue gauge over the baseline window exceeds 1.0, reported not applicable until the gauge scrape exists.
- §8: AC4 also requires that no request intended in the baseline window is unresolved at actual fire, within the 50 ms maximum send skew.

## 2026-08-16

- §3 (adopted 2026-08-15, published in the spec 2026-08-16): the completion curve is the Aalen-Johansen cumulative incidence of completion; errors are competing terminal events at their failure times, and only requests with no terminal event by the pinned timeout or run end are censored. v0.1 used Kaplan-Meier with errors treated as censored. With no errors in a window the estimator equals one minus the Kaplan-Meier survival function.
- §3: a window whose error-plus-censored fraction exceeds 5 percent presents the completion-incidence curve beside any completed-only percentiles, with the caveat explicit.
- §0: a configuration that weakens any pre-registered gate, tolerance or detector parameter does not load.
- Report JSON, a separate document from the configuration file: `schema_version` 2, and `km_curve` is renamed `completion_incidence`.

## 2026-07-30

- §1, §10: the 45 to 55 percent pre-fault share band is run-failing under per-request balancing and recorded under per-connection routing; traffic reaching fewer replicas than declared fails the run in either regime. The client has no connection cap: pool capacity exceeds lambda times the pinned timeout, concurrency demand sets the open-connection count, reconnect on error, no request retries.
- §3: throughput is completions per second per window; goodput is the fraction of a window's scheduled requests completing within the §4 SLO, also reported as SLO-meeting completions per second.

## 2026-07-28

- v0.1, first public version.
