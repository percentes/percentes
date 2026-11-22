# Percentes architecture

Percentes drives open-loop load (each request sent at its scheduled time whether or not earlier ones have answered) at large language model (LLM) inference services and records what the client observes. It runs the replica-loss experiment of [SPEC.md](../SPEC.md), which is authoritative, and the managed-endpoint protocol of [SPEC-HOSTED.md](../SPEC-HOSTED.md), which is subordinate to it. A § number refers to SPEC.md unless it is prefixed SPEC-HOSTED.

Phase 0 runs against the mock and needs no GPU (graphics processing unit); Phase 1 runs against vLLM, the open-source inference server whose version §6 pins, and SPEC.md's status line records which Phase 1 runs are done. Under SPEC-HOSTED, a route is one provider, model identifier, endpoint, account tier and request shape, and a cell is one run of the route's ladder, the stepped series of request rates of SPEC-HOSTED §3.

- [Binaries](#binaries)
- [Package map](#package-map)
- [A run, in order](#a-run-in-order)
- [One request, from schedule to outcome](#one-request-from-schedule-to-outcome)
- [Recovery detection](#recovery-detection)
- [Gates](#gates)
- [Server-side sampling and the receive path](#server-side-sampling-and-the-receive-path)
- [Fault modes](#fault-modes)
- [Not implemented](#not-implemented)
- [Hosted runs and the evidence files](#hosted-runs-and-the-evidence-files)
- [The release runner](#the-release-runner)
- [Tracing a published number back to raw records](#tracing-a-published-number-back-to-raw-records)
- [Changing things safely](#changing-things-safely)
- [Glossary](#glossary)

## Binaries

Packages are each binary's `internal/` imports from `go list`, less `internal/config`.

| Binary | What it does | Packages it drives | Exit codes |
|---|---|---|---|
| `cmd/percentes` | One self-hosted run: `report.json`, `report.txt`, `attempts.jsonl`. Refuses a hosted target, and through `run.CheckConfig` a pilot configuration or a literal `target.body`. `--replay DIR` re-renders the report pair of the release cell whose directory is DIR; `--check-release DIR` runs the completeness check on that directory | `run`, `loadgen`, `serverstats`, `validity`, `evidence`, `report`, `releasekey` | 0 run valid, replay identical or check passed; 2 a run-failing gate; 1 error, a refused target, a replay that differs or a refused check |
| `cmd/percentes-campaign` | N runs (`run.repetitions`) of one fault variant, gates per run, §7 aggregation into `campaign.json` and `campaign.txt`; `--dry-kill` makes one process kill with no load | `campaign`, `run`, `orchestrator`, `collect`, `detect`, `vllmlog`, `serverstats`, `validity`, `loadgen`, `redact`, `report` | 0 every run valid; 2 an invalid run or a halt (`--halt-after-invalid-run`, on by default for process kill); 1 error |
| `cmd/percentes-calibrate` | The §10 capacity calibration and the §5 reference step, rewriting `calibration.json` and `calibration.txt` after every step; `--check` validates a configuration and lists its placeholders | `calibrate`, `serverstats`, `redact`, `report` | 0 valid; 2 completed with no lambda_max (the measured single-replica capacity); 1 error |
| `cmd/percentes-pilot` | The descriptive pilot of hosted amendment 2 (HA2, a draft amendment in `SPEC-HOSTED-AMENDMENT-DRAFT.md`): `ledger` (`create`, `price`, `hosting`, `show`), `probe`, `dev`, `session`, `check-cpu` | `admission`, `probe`, `pilot`, `score`, `evidence`, `loadgen`, `collect`, `run`, `validity` | 0 done; 2 a client-invalid window or one that did not start, a probe blocked, the second window late, the development gate failed, or `check-cpu` found host CPU (central processing unit) unmeasured; 1 error |
| `cmd/percentes-release` | One release route per process, and the commands before and after its window ([The release runner](#the-release-runner)) | `admission`, `schedule`, `probe`, `pilot`, `evidence`, `report`, `releasekey`, `loadgen`, `mock`, `redact` | per subcommand, in [its table](#the-release-runner) |
| `cmd/mockserver` | The Phase 0 mock inference server from a run configuration's `mock` section; `--addr-file` writes the bound address | `mock` | 0 stopped by SIGINT or SIGTERM (the interrupt and termination signals); 1 error |
| `cmd/naivesweep` | A closed-loop sweep (each of a fixed number of workers sends its next request once its last one has answered) of an OpenAI-compatible endpoint, configured by `SWEEP_ENDPOINT`, `SWEEP_API_KEY` and `SWEEP_MODEL`; outside the instrument, with no client-validity gate and no ledger admission | `sse`, `redact` | 0 every request has a verdict; 1 configuration missing or invalid |

## Package map

| Package | Responsibility | Key files, types and entry points | § |
|---|---|---|---|
| `internal/config` | The configuration schema; strict YAML (YAML Ain't Markup Language) decode; the pins of each profile (`experiment`, `ac`, `pilot`, `release`) checked at load; the configuration as published, confidential values withheld | `config.go` (`Config`, `Target`, `Pinned*` constants), `load.go` (`LoadFile`, `Decode`), `validate.go` (`Config.Validate`), `release.go` (`ReleaseBodyAt`, `Target.ExpectedModel`), `published.go` (`Config.Published`, `ValueForms`, `Carries`, `CarriesOutside`, `FixedIdentifiers`, `RetainedError`), `request.go` (`LiteralBody`) | §6, §8, §10; SPEC-HOSTED §3, §9 |
| `internal/loadgen` | Schedule, pacer, server-sent events (SSE) client, outcomes, client-validity gate, canary, release ladder, tripwires (checks on a send beyond the schedule or too many requests in flight, which end a release ladder and invalidate a pilot window), write limit | `schedule.go` (`BuildSchedule`, `BuildLadder`), `loadgen.go` (`Run`, `Hooks`, `Request`, `NewTransport`), `sse_client.go` (`gen.execute`, `recordChunk`), `gates.go` (`evaluateGates`, `CPUMeasured`), `canary.go` (`StartCanary`, `SummarizeCanary`), `write_permits.go` (`newWriteWindows`), `write_time.go` (`watchRelease`, `WatchWrite`, `instrumentWrites`) | §2, §3; SPEC-HOSTED §3, §5, §7 |
| `internal/sse` | SSE framing with an event-size limit the caller sets, for the client, the probes and `cmd/naivesweep` | `sse.go` (`SplitLines`, `Events`, `EventsCut`) | §3 |
| `internal/histo` | The pinned HdrHistogram (high-dynamic-range histogram) configuration; values enter only through `H.Record`, which calls the library's plain `RecordValue` | `histo.go` (`New`, `H.Record`, `H.Summarize`) | §3 |
| `internal/mock` | OpenAI-compatible SSE mock with five scheduled fault modes and a slow reload; the shaped hosted-provider form; TLS (Transport Layer Security) serving; a 429 rate limit | `server.go` (`New`, `Server.Start`, `Server.StartTLS`), `faults.go`, `sse.go`, `shape.go` (`NewShaped`, `Shape`), `messages.go`, `limit.go` (`Server.SetRateLimit`), `tls.go` (`SelfSignedCert`) | §2; SPEC-HOSTED §9 |
| `internal/orchestrator` | Pre-armed fault execution with armed, fire and expiry records; mock-admin, clean-delete and process-kill injectors; container commands over SSH (Secure Shell) | `orchestrator.go` (`Execute`, `NewMockInjector`), `injectors.go` (`NewCleanDeleteInjector`), `processkill.go` (`NewProcessKillInjector`), `ssh.go` (`SSHContainerOps`) | §1, §2; §8 acceptance criterion (AC) 3 |
| `internal/serverstats` | Prometheus sampling, per-window reductions, the baseline waiting-queue means the G7 gate (the §10 queue-stability gate) reads | `serverstats.go` (`ForRun`, `Sampler.Start`, `Preflight`, `BaselineMeans`, `ReduceWindow`) | §2, §10 |
| `internal/collect` | Windows, completion-incidence curves, in-flight accounting, threshold analysis, tail confidence intervals (CIs), receive-path report | `collect.go` (`Collect`, `AccountInFlight`, `SplitAtFire`, `AnalyzeThresholds`, `FireAnchorNs`, `GuardStartNs`), `incidence.go` (`EstimateIncidence`), `receivepath.go` | §3, §4, §7 |
| `internal/detect` | Recovery detector, decomposition, recovery probes, `/health` calibration | `detect.go` (`BuildSeries`, `RunWith`, `Run`, `NewDecomposition`), `probe.go` (`ProbeRecovery`), `calibration.go` (`CalibrateHealth`) | §5 |
| `internal/vllmlog` | vLLM start-log boundaries for the process-kill decomposition | `patterns.go` (`VLLM0290`), `vllmlog.go` (`Parse`), `apply.go` (`Apply`) | §5 |
| `internal/validity` | The G1 to G7 run-validity gates of §10; Hosted-G6 (completion within the deadline) per release step | `validity.go` (`Evaluate`, `G2`, `ReleaseG6`) | §10; SPEC-HOSTED §7 |
| `internal/stats` | §7 statistics: values, median, mean, t-interval, coefficient of variation; Holm step-down (unused) | `stats.go` (`Summarize`), `holm.go` | §7 |
| `internal/run` | One run's artifacts from load to validity; the configurations the single-run path refuses | `run.go` (`Execute`, `analyze`, `CheckConfig`, `Artifacts`), `replay.go` (`Replay`), `container.go` | §1 to §5 |
| `internal/campaign` | N runs, per-run scalars, endpoint summaries with drop counts, halt after an invalid run | `campaign.go` (`Run`, `RunWith`) | §5, §7 |
| `internal/calibrate` | The §10 ramps and the §5 reference step | `calibrate.go` (`RunRamp`, `Calibrate`, `Reference`), `runner.go` (`LoadRunner`), `output.go` | §5, §10 |
| `internal/report` | Report pairs for runs, campaigns, hosted runs and release cells; the pilot report; per-step tables, `status_429` attribution, null reading, drift split, index | `report.go` (`Generate`), `campaign_report.go` (`GenerateCampaign`), `replay.go` (`FromRun`, `WriteHostedPair`), `pilot.go` (`GeneratePilot`), `cell.go` (`CellReport`), `steps.go` (`NewStepTables`), `attribution.go` (`StepTables.Attribute`, `RecordSends`, `ExchangeSend`), `null.go`, `drift.go` (`DocsOf`, `CompareWindowClose`), `index.go` (`NewIndex`) | §2 to §5, §7; SPEC-HOSTED §4, §5, §8 |
| `internal/redact` | Credentials and response bytes kept out of printed and written errors; a network error's text written with this host's source address removed as [the error text](#hosted-runs-and-the-evidence-files) gives it | `redact.go` (`URL`, `ErrorText`, `Wrap`) | §6 |
| `internal/probe` | The hosted pre-run probe and start gate; the release ceiling check, its record and the ceiling choice; withheld values | `probe.go` (`Run`, `Gate`, `Write`, `Encode`, `WriteRetained`), `release.go` (`RunRelease`, `ReleaseRecord.Verify`, `ChooseCeiling`, `ChooseCeilingAt`, `Decisive`, `Spent`, `ModelFault`, `RepeatQuiet`, `Exposed`, `ExposedByMarking`), `shape.go` | SPEC-HOSTED §2, §5, §9 |
| `internal/admission` | The hash-chained ledger of requests and their worst-case cost (liability); the release ceiling rule with its binding-rate rule | `ledger.go` (`Create`, `Open`, `Parse`, `Ledger.Bind`, `Ledger.AdmitAt`, `Ledger.Settle`, `PilotCaps`, `ReleaseCaps`), `ceiling.go` (`Solve`, `CeilingRule`, `CheckRPS`, `WindowAllowances`, `CheckPublished`, `ExpectedPlan`), `sent.go` (`ProbeSent`) | SPEC-HOSTED §3, §9 |
| `internal/evidence` | Per-attempt records, frame store (the retained reads of each response body), run records, operator file, manifest, completeness check, evidence pack, publication record and page | `attempts.go` (`Records`, `WriteRun`), `frames.go` (`WriteRunFrames`, `CheckFramesRoot`), `clock.go` (`ReadClock`), `status.go` (`ReadStatus`, `ReadBalance`), `balance.go` (`CheckBalance`), `refresh.go` (`ReadRefreshedLimits`, `NewWindowClose`), `operator.go` (`LoadOperator`), `manifest.go` (`NewManifest`, `ReleaseG6`), `cell.go` (`Cell`), `gaps.go` (`StepGaps`), `release.go` (`CheckRelease`), `pack.go` (`BuildPack`), `publication.go` (`LoadPublication`, `Publication.Check`), `publication_page.go` (`Publication.Page`) | SPEC-HOSTED §4, §9 |
| `internal/pilot` | Synthetic ticket text: the pilot fixture and schedule, Shape S tickets (the release's support-ticket request shape, input bound 1,200 tokens), the Shape R text (its short fixed instruction, input bound 128 tokens), the shape factory | `tickets.go` (`Generate`), `schedule.go` (`NewSchedule`), `shape.go` (`GenerateShapeS`), `release_shape.go` (`GenerateShapeR`, `NewShape`) | SPEC-HOSTED §3; HA2 |
| `internal/score` | The pilot's output contract and its ten task outcomes | `route.go` (`NewContract`), `outcome.go` (`NewScorer`, `Scorer.Score`) | HA2 |
| `internal/schedule` | Each route's sampling frame (its twelve cells, SPEC-HOSTED §4) from one master seed: dates, attempt seeds, offsets, re-runs, the frozen inputs | `schedule.go` (`New`, `Load`, `Schedule.Runs`) | SPEC-HOSTED §4 |
| `internal/releasekey` | The release key and HMAC-SHA256 (a hash-based message authentication code over SHA-256, the 256-bit Secure Hash Algorithm) under it | `releasekey.go` (`Create`, `Read`, `Key.Sum`) | SPEC-HOSTED §9 |
| `internal/hostqual` | The host probe the timing-coupled acceptance tests run first | `hostqual.go` (`Measure`, `Allocation`, `Qualified`) | §2, §8 |
| `internal/ac` | The §8 acceptance suite, with the mock in a separate process | test files only | §8 |

![Package dependencies from go list: the seven commands with their internal imports, and the internal packages grouped by role](diagrams/package-layers.drawio.svg)

| Path | Contents |
|---|---|
| `configs/` | Every runnable configuration; `experiment.reference.yaml`, `ac.reference.yaml`, `pilot.reference.yaml` and `release.reference.yaml` are the profiles' reference files; `embed.go` builds `release.reference.yaml` into `percentes-release` for `rehearse` |
| `deploy/kind/` | The kind (Kubernetes in Docker) cluster configuration, `smoke.sh`, `reproduce.sh` (AC7) and `campaign-e2e.sh` |
| `deploy/mock/` | The two-replica mock Deployment and its NodePort Service (a Service reachable on one fixed port of every cluster node) |
| `deploy/phase1/` | The vLLM topology manifest and the Phase 1 capture scripts |
| `deploy/release/` | The release's refresh command, which a release's freeze commit adds to the tree: `pct-refresh.py` and one `refresh-<route>` wrapper per provider it reads rows for, where `<route>` is the `target.name` of that provider's route; a second route of the provider, such as `azure-R`, names the first's record files in its operator file and runs its wrapper. A wrapper runs `pct-refresh.py` for its route and is the command `--refresh-cmd` names. The script rewrites the route's limits and eligibility records with the rows of its tier and model from the provider's pages, keeps each page's text beside them, and prints `{"limits": [...]}`. For a route whose rows it does not read, it prints every limit as unavailable |
| `docs/diagrams/` | draw.io diagrams as SVG (Scalable Vector Graphics) files that carry their draw.io source |

## A run, in order

![One run of cmd/percentes: the setup before the load, the client host and the target cluster during the run, and the analysis pipeline after the last terminal event, with percentes-campaign repeating the run](diagrams/system-flow.drawio.svg)

`cmd/percentes/main.go`, then `run.Execute` (`internal/run/run.go`):

1. `config.LoadFile` decodes and validates the file; `run.CheckConfig` and the `target.hosted` check refuse what this binary does not run.
2. `serverstats.Preflight` checks the queue gauge, the kept families and the time-to-first-token (TTFT) histogram on each `target.metrics_urls` endpoint; a failure exits 1 before any dispatch.
3. `loadgen.Run` builds the schedule, anchors the run epoch on the monotonic clock and calls `Hooks.OnEpoch`. `run.Execute`'s hook releases the orchestrator and the probes, then calls `Options.OnEpoch`, through which `cmd/percentes` starts the Prometheus sampler (`Sampler.Start`) and the loopback canary (`loadgen.StartCanary`).
4. Where the run has an injector, `orchestrator.Execute` pre-arms it to fire at T_inject, the configured fault instant (the warm-up plus `fault.t_inject_offset_s` after the epoch), and records armed, fire and expiry times. `percentes-campaign` passes its clean-delete or process-kill injector, and `--admin-url` arms the mock-admin one (`orchestrator.NewMockInjector`). A mock with a `mock.fault_schedule` arms its own faults at start-up.
5. Where `--probe-direct` or `--probe-service` is given, `detect.ProbeRecovery` polls from T_inject every 500 ms until its first counted success or the end of the fault phase.
6. The load runs through the configured `run.phases`: warm-up, baseline, the fault phase and cooldown. The experiment profile pins them at 60, 300, 600 and 60 s (`PinnedWarmupS`, `PinnedBaselineS`, `PinnedFaultWindowTimeoutS`, `PinnedCooldownS`). Monitors sample client CPU at 1 Hz and the Go runtime's garbage-collection (GC) pause histogram.
7. After the last terminal event, `run.Execute` reads a mock's scheduled fires back from `/admin/faults` (`scheduleFires`) and calls `analyze`, which:
   1. collects the `baseline`, `guard` and `fault` windows with `collect.Collect`;
   2. runs the detector, `detect.RunWith`, over `detect.BuildSeries`;
   3. collects `fault_survivor`, and `fault_degraded` and `fault_recovered` when recovery falls inside the fault window;
   4. computes `collect.AccountInFlight`, the share gate (`shareGate`) and `collect.AnalyzeThresholds`;
   5. fills the decomposition (`detect.NewDecomposition`) from the probe times;
   6. decides validity in `validity` from the fault source, G2 (client validity), G1 (replica share) and the injection timing.
8. The binary stops the sampler and canary, calls `Artifacts.AttachObservations` (top-level `server_side` and `receive_path`), evaluates G1 to G7 (`validity.Evaluate`), writes `attempts.jsonl` (`evidence.WriteRun`) and the report pair (`report.Generate`), and exits 0, 2 or 1.

The fire anchor is whichever comes first of T_inject and the recorded fire (`collect.FireAnchorNs`). `configs/experiment.reference.yaml` sets `fault.t_inject_offset_s` to 300 s, equal to the baseline phase, so T_inject is 360 s from the epoch.

| Window (report key) | Bounds, by intended dispatch time, half-open | Built in |
|---|---|---|
| `baseline` | [warm-up end, fire anchor − `client.http_timeout_s`) | `collect.GuardStartNs` |
| `guard` | [fire anchor − `client.http_timeout_s`, T_inject) | `collect.GuardStartNs` |
| `fault` | [T_inject, end of the fault phase) | `loadgen.Run` sets the phase ends |
| `fault_degraded`, `fault_recovered` | split at the detector's recovery point | `analyze` |
| `fault_survivor` | the fault window, requests served by the one replica besides the victim, the replica the fault targets (`survivorOf`) | `analyze` |
| `outage` | process kill only: from the fire to `replica_ready` | `cmd/percentes-campaign` |

![One run to scale: phases, report windows, the fault-window splits and the detector's plateau, with the guard window magnified for an on-time and an early fire](diagrams/run-timeline.drawio.svg)

**Campaigns.** `campaign.RunWith` runs N repetitions (`PinnedRepetitions`, 5, on the experiment profile); each repetition's seed is `run.seed` plus its index from 0. Per-run scalars aggregate through `stats.Summarize`.

§1 has three fault variants. A clean delete removes the victim's pod with no grace period; it arms `orchestrator.NewCleanDeleteInjector` per run against the pod `--victim` names or one `--victim-selector` picks. A black hole cuts the victim's whole node off the network for a fixed time; `percentes-campaign` refuses it, since no node-partition injector exists. A process kill sends SIGKILL (signal 9) to a single replica's serving process, and its container restarts in place.

**Calibration.** `calibrate.Calibrate` runs two `calibrate.RunRamp` ramps against one replica, a third where they disagree by more than `PinnedCalibrationAgreementFrac`, freezes lambda_r, the per-replica arrival rate, at `PinnedLambdaRFrac` (0.65) of lambda_max, and `calibrate.Reference` runs the §5 step at 2 × lambda_r.

![The §10 calibration procedure as a flowchart](diagrams/calibration.drawio.svg)

## One request, from schedule to outcome

1. **Schedule.** `loadgen.BuildSchedule` (self-hosted) or `loadgen.BuildLadder` (release) fixes every intended dispatch time t_i before the run (`internal/loadgen/schedule.go`).
2. **Pacer.** One goroutine (a Go lightweight thread) sleeps until t_i − 10 ms (`spawnLeadNs`), spawns request i's worker and moves to request i + 1 (`internal/loadgen/loadgen.go`).
3. **Worker.** The worker sleeps on its own timer (`gen.sleep`) until t_i − 1.5 ms (`spinNs`), spins on the clock to t_i and calls `gen.execute`. A worker spawned late skips the timer; one spawned after t_i dispatches at once, and its lateness is send skew.
4. **Dispatch.** `gen.execute` (`internal/loadgen/sse_client.go`) stamps `dispatch_ns`. Under the release profile a request more than `PinnedSendSkewMaxMs` (50 ms) past t_i is not sent and stays undispatched. The per-request timeout (`clientTimeout`) runs from dispatch: `PinnedClientTimeoutS` (30 s), or `client.http_timeout_s` (90 s) under the release profile.
5. **Body and header.** `gen.requestBody` builds the generated body, the literal `target.body` with its case slot `"{{case}}"` replaced (`config.LiteralBody`), or a pilot configuration's body (`Pilot.RequestBody`). `gen.newRequest` posts to `target.endpoint_url`, else `target.base_url` plus `/v1/chat/completions`, with the key in the header `Target.KeyHeader` gives.
6. **Stream.** The `httptrace` `GotConn` hook records `peer_addr`, `conn_reused` and `conn_acquired_ns`; `watchRelease` records `wrote_request_ns`. `parseChunk` reads chat-completions chunks at the paths `target.response_shape` names, and `parseMessagesEvent` reads a Messages stream. The `sse.Events` callback in `gen.execute` stamps `first_token_ns` at the first content event and records each later gap as an inter-token latency (ITL). `recordChunk` merges usage objects (`mergeUsage`) and keeps the finish reason, the reported model, the system fingerprint and the first in-band error object.
7. **Outcome.** The request ends in one of the three §3 states (`OutcomeCompleted`, `OutcomeErrored`, `OutcomeCensored`); an errored request carries one of six error classes. Under the release profile a completion after `release.shape.deadline_s` (30 s from t_i) is marked `late` (`releaseDeadline`).
8. **Collection.** `collect.Collect` assigns the request to the window of its t_i, records completed TTFT and end-to-end (e2e) latency, both measured from t_i, through `histo` and adds every scheduled request to the window's incidence curve. `detect.BuildSeries` buckets the same records at 1 s for the detector.

![The pacer and worker timing for one request, including a late pacer wake-up that still dispatches at t_i](diagrams/pacer-timing.drawio.svg)

| Error class | Set where | Condition |
|---|---|---|
| `connect` | `classifyTransportErr`, `gen.execute` | any transport failure other than a deadline or a reset, a refused write among them, or a request that could not be built |
| `reset` | `classifyTransportErr`, `classifyStreamErr` | `ECONNRESET` (the peer reset the connection) |
| `status_429` | `gen.execute` | a final status of 429 |
| `status_other` | `gen.execute` | any other non-200 status (the client follows no redirect) |
| `malformed_stream` | `classifyStreamErr`, `gen.execute` | an end before the terminator (`[DONE]`, or `message_stop` on a Messages target), a read error other than a reset or a deadline, an undecodable event, an event over `eventLimit` (1 MiB), or, on a hosted target (`target.hosted`), the first in-band error object |
| `empty_stream` | `gen.execute` | the terminator with no content event |

An in-band error object is an error object carried inside a stream answered 200. On a self-hosted target `recordChunk` keeps the first one in `stream_error` and the stream is read on to its terminator, so the request can still complete or end as `empty_stream`.

A request with no terminal event by its timeout is censored (`context.DeadlineExceeded`). The diagram shows the chat-completions stream and the 30 s timeout.

![The outcome paths of the request traced above: its failure stages and their tests, the error class each failure takes, the censored and completed states, the order of precedence among them, and the outputs each state reaches](diagrams/outcomes.drawio.svg)

## Recovery detection

`internal/detect/detect.go` computes the `detector` and `decomposition` fields of a self-hosted report. A hosted report carries neither.

| Input or rule | Value | Source |
|---|---|---|
| Series | goodput (the fraction of scheduled requests meeting the §4 objective, TTFT ≤ `PinnedSLOTTFTMs` and e2e ≤ `PinnedSLOE2EMs`) per 1 s bucket of intended time | `BuildSeries` |
| Window R | 10 s, leading: the window at t covers [t, t + R) | `PinnedDetectorWindowS` |
| Entry | a window at or above 90 percent of the applicable baseline | `PinnedDetectorEntryPct` |
| Hold H | 30 s; any window below entry in the hold cancels the candidate (`canceled_entries`) | `PinnedDetectorHoldS` |
| Exit | a window below 85 percent after recovery starts a re-degradation (`re_degradations`) | `PinnedDetectorExitPct` |
| Pre-fault baseline | the `baseline` window's goodput | `collect.Collect` |
| Equilibrium baseline | ratio-of-sums goodput from fire anchor + R to the pre-fault entry or the timeout | `RunWith` |
| Sweep | entry 85, 90, 95 percent; R 5, 10, 20 s; H 15, 30, 60 s; 27 rows (`sensitivity`) | `PinnedSensitivityEntryPct`, `PinnedSensitivityWindowS`, `PinnedSensitivityHoldS` |
| Partition heal | black hole only: the first held entry starting no earlier than the heal anchor, which is the observed expiry or fire anchor + `PinnedPartitionDurationS` (`partition_heal_recovery`, `heal_anchor_ns`) | `RunWith` |

| Output | Meaning |
|---|---|
| `to_pre_fault`, `to_equilibrium` | time to recovery (TTR) per baseline: the held entry's start minus the fire anchor |
| `hold_unobserved` | a candidate entry window or hold that runs past the end of the series |
| `components` | separate detections for `ttft_slo`, `e2e_slo` and `error_rate` |
| `backlog_drain_*` | always reported unmeasured |
| `decomposition.segments` | each §5 boundary with its source and a note where unmeasured (`NewDecomposition`) |

`NewDecomposition` reports the Kubernetes API (application programming interface) and log boundaries N/A (not applicable) for the clean-delete, black-hole and mock variants. Under process kill, `cmd/percentes-campaign` fills container start from `docker inspect` and the log rows through `vllmlog.Apply`. The probes supply `replica_ready`, and `traffic_restored` where a Service exists, each measured from the fire.

![Detector entry, hold and exit on a sketched goodput series](diagrams/recovery-detector.drawio.svg)

## Gates

| Gate | Checks | Computed in | On failure | § |
|---|---|---|---|---|
| Configuration pins | each SPEC number as an equality or bound | `internal/config/validate.go`, `Config.Validate` | the file does not load | §6; SPEC-HOSTED §3 |
| Metric preflight | queue gauge, kept families, TTFT histogram type | `internal/serverstats/serverstats.go`, `Preflight` | exit 1 before dispatch | §2 |
| G1 share | 45 to 55 percent per replica over the baseline (`PinnedShareMinPct`, `PinnedShareMaxPct`), and the declared replica count seen | `internal/run/run.go`, `shareGate`; `internal/validity/validity.go`, `gateG1` | run invalid; the band is descriptive under per-connection routing (`config.BalancesPerRequest`); not applicable to one replica or a hosted target | §1, §10 |
| G2 client validity | over the measured span (warm-up end to fault-phase end, or a release step's measured window): send skew 99th percentile (p99) ≤ 5 ms and maximum ≤ 50 ms, client CPU ≤ 70 percent over every 5 s window (an unmeasured CPU fails) and GC pause p99 < 1 ms. Over every request judged (the whole run; in a release step, its requests, with the warm-up's on step 1; over a ladder, every request through its last attempted step): no refused write and no undispatched request | `internal/loadgen/gates.go`, `evaluateGates` | run invalid; in a release step, the ladder ends and the cell is `client_invalid` | §2, §10; SPEC-HOSTED §7 |
| Fault source | an armed injector, or the mock's scheduled fires read back | `internal/run/run.go`, `validity` | run invalid | §2 |
| Injection timing | fire within `PinnedInjectionToleranceMs` (500 ms) of T_inject | `internal/run/run.go`, `validity` | run invalid | §8 (AC3) |
| G3 | no errored victim-attributed in-flight request (black hole only) | `internal/validity/validity.go`, `gateG3` | strips the node-loss-representative label (`node_loss_representative`, the §1 label marking a black-hole run as standing for node loss) | §1, §10 |
| G4 | staleness window ≥ `PinnedEndpointStalenessMinS` (20 s) with victim-bound traffic observed inside it | `gateG4` | strips the label; no producer fills the G4 observation | §10 |
| G5 | GPU fingerprints equal | `gateG5` | not applicable: the fingerprints `percentes-campaign` writes under process kill are not fed into the gate | §10 |
| G6 | baseline goodput ≥ `PinnedBaselineGoodputMin` (0.99) | `gateG6` | run invalid | §10 |
| Hosted-G6 | completion within 30 s over the `baseline`, `guard` and `fault` windows; per release step, completion within the deadline | `gateG6Hosted`; `validity.ReleaseG6` through `evidence.ReleaseG6` | reported only | §6; SPEC-HOSTED §7 |
| G7 | per-replica waiting-queue mean ≤ `PinnedQueueGaugeMax` (1.0) with ≥ `PinnedQueueCoverageMin` (90 percent) of samples | `gateG7` | run invalid; not applicable without `target.metrics_urls` | §10 |
| Release send skew | a worker more than `PinnedSendSkewMaxMs` (50 ms) past t_i | `gen.execute` | request unsent; its step's G2 fails | SPEC-HOSTED §3 |
| Release write limit | per evaluation window W, at most the lesser of ceil(the top step's rate × (W + 0.1 s)) writes and the window's allowance (`admission.WindowAllowances`); a write that some window does not admit waits for a permit, for at most `writePermitWaitNs` (50 ms) from its first attempt | `internal/loadgen/write_permits.go`, `newWriteWindows`, `tryReserve` | the write is refused with no byte sent where no permit comes within that wait, the next permit would come after it, or the write wakes from its wait after it. The request is errored as `connect` with `write_denied` true, and G2 fails | SPEC-HOSTED §3 |
| Release step verdict | G2 over the step, a tripwire, a `status_429` fraction above 0.5 | `internal/loadgen/loadgen.go`, `stepEnd` | the ladder ends (`client_validity`, `tripwire`, `backoff`) | SPEC-HOSTED §3, §5 |
| Tripwires | sends beyond a step's schedule; in flight above 2 × the step's rate × `client.http_timeout_s` (the hard timeout, 90 s) (`inFlightLimit`) | `internal/loadgen/loadgen.go`, `newLadderTripwire` | the ladder ends; the cell is `client_invalid` | SPEC-HOSTED §5 |
| Client exhaustion | `EMFILE` (no file descriptor left) or `EADDRNOTAVAIL` (no local port left) | `cmd/percentes-release/stops.go`, `clientExhausted` | the ladder ends; the cell is `client_invalid` | SPEC-HOSTED §2, §5 |

`validity.Evaluate` evaluates only G2 and Hosted-G6 for a hosted target and reports the rest not applicable.

| Build | Binaries | Host CPU |
|---|---|---|
| `make bins` | `percentes`, `percentes-campaign`, `mockserver`, `percentes-calibrate`, with `CGO_ENABLED=0` | unmeasured on macOS: a run of `percentes`, `percentes-campaign` or `percentes-calibrate` there fails G2 |
| `make percentes-pilot`, `make percentes-release` | the pilot command and the release runner, built on macOS with cgo (Go's interface for calling C code), through which they read host CPU | measured |
| `make percentes-release-linux` | the release runner for the Linux client host, static, with `CGO_ENABLED=0` | measured from `/proc` |

`percentes-release check-cpu` reports whether a build measures host CPU.

Before each timing-coupled acceptance test, `hostqual.Qualified` measures timer wake lateness and GC pauses (`hostqual.Measure`) and checks them against `hostqual.Allocation`. A test skips on a host that fails. After a failed timing assertion the host is measured again: a host that lost qualification skips the criterion, and one that kept it fails. `make test-ac` fails on any skip unless `PERCENTES_AC_ALLOW_SKIPS=1`.

## Server-side sampling and the receive path

| Setting | Use | Code |
|---|---|---|
| `target.metrics_urls` | one Prometheus endpoint per replica, keyed r0, r1 in order, sampled every `PinnedQueueSampleIntervalS` (1 s) from the epoch | `serverstats.ForRun` |
| `target.queue_gauge` | the waiting-queue gauge G7 reads (`vllm:num_requests_waiting` on vLLM, `percentes_mock_requests_waiting` on the mock) | `serverstats.BaselineMeans` |
| `target.metrics_families` | families kept on every sample and reduced per window and replica into `server_side` | `serverstats.ReduceWindow` |
| `target.ttft_histogram` | the server TTFT histogram the receive-path check compares with | `collect.ReceivePathFor` |

| Family type | Per-window value in `server_side` |
|---|---|
| gauge, untyped | mean over the samples inside the window |
| counter | increase per label set from its last sample before the window to its last inside, summed |
| histogram | increase in count, sum and buckets by the same rule, the +Inf bucket left out |

A value that falls for a label set, or a changed bucket layout, is a reset; a non-finite value or an overflowing sum counts in `family_errors`. The sample series is under `server_samples`.

The §2 receive-path checks are reported per window in `receive_path` and fail no run: the client TTFT mean against the server histogram's mean, and the loopback canary (`loadgen.StartCanary`), a stream that a mock inside the client process serves at fixed times and the load's own reader parses. A hosted target exposes no metrics endpoint, so only the canary applies.

A release cell carries one canary summary per attempted step in its cell record, checked on replay (`stepCanary`), and a canary that does not start makes the cell `client_invalid`.

| Case | Pooled gap label begins (`itlLabel`, after `ITL pooled (per-window),` in `report.txt`) |
|---|---|
| hosted target | `inter-chunk (§3` |
| usage sampled, every completion's count equals its content-event count | `inter-token (§10 check:` |
| usage sampled, a mismatch | `inter-chunk (§3; §10 check:` |
| mock, no usage in the stream | `one token per content event by construction` |
| any other target with no usage | `inter-chunk (§3, no usage in the stream)` |

Each window reports `content_events`, `completion_tokens` and the §10 check over completions that carried a count (`token_check`).

## Fault modes

| §2 name | Configuration | Mock behaviour | Expected signature |
|---|---|---|---|
| stall | `stall` | server-wide emission freeze, staggered flush on expiry | completions delayed into p99.9 and max (AC2, AC2b) |
| error | `error` | 500 on new requests; in-flight streams untouched | an error-rate step and a detector TTR |
| throttle | `throttle` | 429 on new requests | `status_429` in the fault window only |
| stream-abort | `stream_abort` | a TCP (Transmission Control Protocol) reset (RST) of in-flight streams, closing with `SO_LINGER=0` | in-flight requests errored as `reset` (AC4) |
| silent-hang | `silent_hang` | sends nothing more on hijacked connections and holds them open | censored at the timeout, on the curve (AC4b) |
| slow reload | `mock.slow_reload` | 503 for a set time after start | the `replica_ready` boundary |

The first five are scheduled in `mock.fault_schedule` or armed through `/admin/faults` (`internal/mock/faults.go`, `internal/mock/server.go`); slow reload is a start-up setting.

**Process kill** (`cmd/percentes-campaign/processkill.go`, `internal/orchestrator/processkill.go`):

1. Before each run:
   1. wait until the container runs, `/health` returns 200 and one streamed inference succeeds;
   2. read the container's process ID (PID), start time, restart policy and restart count with `docker inspect`;
   3. check the restart policy and container name against `pins.container` (`pinReasons`), and that the container did not restart since the last run;
   4. write the before fingerprint;
   5. measure the host clock's offset (`SSHContainerOps.ClockOffset`).
2. At T_inject, `ProcessKillInjector` checks the PID still belongs to the container and sends SIGKILL through `SSHContainerOps.KillInit`; the recorded fire is the midpoint of the host-clock bracket around the kill, converted by the offset (`observedFire`).
3. Docker's restart policy (`PinnedContainerRestartPolicy`, `on-failure`) starts the same container.
4. After the run, check the restart count advanced by exactly one, read the `die` and `start` events, the server log (`vllmlog.Parse`, `vllmlog.Apply`) and the after fingerprint; `collect.SplitAtFire` separates requests indeterminate at the fire, and the `outage` window is collected. Any failed check or read makes the run invalid.

Commands run over SSH on `--ssh-target`, or locally when it is empty; a local kill runs through `sudo` or, with `--kill-via docker-helper`, a privileged helper container (Docker on macOS). `deploy/process-kill-e2e.sh` (`make process-kill-e2e`) kills the mock container once with `--dry-kill` and again in a two-run campaign.

## Not implemented

Not implemented for real vLLM:

- replica attribution from the §1 layer-7 proxy (a proxy that routes each HTTP request to a replica); `loadgen` reads the serving replica only from the mock's `X-Percentes-Replica` header;
- the node-partition injector;
- producers for the G4 and G5 observations (`validity.Observations`);
- a backlog-drain measurement;
- the Kubernetes API and log boundaries of the decomposition under clean delete, which `NewDecomposition` reports N/A. Under process kill, `cmd/percentes-campaign` fills container start and the log rows, and the reschedule boundary is N/A under §5. Under black hole, §5 makes these boundaries N/A.

No report or campaign applies the §7 Holm step-down to secondary comparisons; `stats.holm` is unexported and called only from its tests.

## Hosted runs and the evidence files

A hosted target (`target.hosted`) is a managed provider endpoint, driven by `cmd/percentes-pilot` and `cmd/percentes-release`; each request is admitted in the admission ledger before its send. The key is read from the file `--env` names (`NAME=value` lines, owner-readable only), as the variable `target.api_key_env` names, and is masked out of every recorded header, body and error string.

| File | Writer | Content | Published or retained |
|---|---|---|---|
| `attempts.jsonl` | `evidence.WriteRun` through `evidence.Records` | one record per scheduled request in index order: times, outcome, class, status, allowed headers, usage, `finish_reason`, `reported_model`, `stream_error`, and `err_text` in the form `redact.ErrorText` gives (table below). Under the release profile `shape`, `ceiling_tokens`, `step`, `warmup`, `in_time`, `late`, `wrote_request_ns`, `write_denied`, `billing_refusal`, `reasoning_tokens` and no answer text | published |
| `frames.jsonl` | `evidence.WriteRunFrames` | every read of a 200 body with its time, up to `frameLimit` (8 MiB) per request, and the first `errorBodyLimit` (4,096) bytes of a non-200 body. Retained header values. Only byte counts (`dropped_bytes`) where `target.retention.model_output` is `none`. The in-band error text and the error body's code and status that `attempts.jsonl` withholds where the route has a confidential limit | retained under `--frames` |
| `clock.json` | `evidence.ReadClock`, `evidence.WriteClock` | the clock-sync command (`sntp` or `chronyc tracking`), its output, the parsed offset and its flag | published |
| `status-start.json`, `status-end.json` | `evidence.ReadStatus`, `evidence.WriteStatus` | the operator's endpoint-status command, output verbatim | published |
| `refresh.json` | `evidence.PublishedRefresh`; as run, `evidence.WriteRetainedRefresh` | release cells: the run of the refresh command (`deploy/release/`) and the limits it printed, each confidential limit without its value. Where the route has a confidential limit (`config.Target.Confidential`), `[retained]` in place of standard output, standard error and each limit's `unavailable` reason | published; where the route has a confidential limit, the record as run is retained beside the cell's frames |
| `balance-start.json`, `balance-end.json` | `evidence.ReadBalance`, `evidence.CheckBalance`, `evidence.WriteStatus` | release cells: the balance command; the start record's `check` (`sufficient`, `insufficient`, `unavailable`, `check_failed`) | published |
| `probe.json` | `probe.Write` (pilot window); `probe.Encode` and `replaceFile` (release cell) | the probe exchange, response shape, findings, start decision and reported prompt and output counts | published |
| `probe-retained.json` | `probe.WriteRetained` | what a probe or ceiling-check record withholds: retained header values, a withheld limits body, and each non-200 body and in-band error where the route has a confidential limit | retained |
| `manifest.json` | `evidence.NewManifest` | build revision, configuration hash, endpoint as configured and observed, operator records, ledger hash, every artifact by `sha256` and `bytes` or by `hmac_sha256`, validity, gates, and the release cell record | published |
| `operator.json`, `records/` | `Operator.WriteCopies` | the operator file and records as read | retained beside the frames |
| `config.yaml` | `evidence.WriteConfigCopy` | release, route with a confidential limit: the configuration as loaded | retained, owner-readable |
| `operator-error.txt` | `evidence.WriteOperatorError` | route with a confidential limit: an operator or refresh error that can quote retained text | retained |
| `window-close.json` | `windowClose`, `evidence.NewWindowClose` | per release route after its last cell: the refresh record as published, the tier, and the operator file and records read after it | published; copies under `--frames` in `window-close/<route>/<UTC time>/`, where `<UTC time>` is the record's time in UTC (Coordinated Universal Time), written as the date YYYYMMDD, the letter T, the time HHMMSS to nine decimal places of a second, and the letter Z |

`redact.ErrorText` gives the text of a transport or stream error, in `attempts.jsonl` (`err_text`) and in the probe and ceiling-check records. The first row that matches applies, so a dial or read timeout carried by a `*net.OpError` prints that error's text.

| Error | Text |
|---|---|
| a network error (`*net.OpError`) | its text, without its own source address or that of a wrapped network error that names both ends |
| a certificate that does not verify | its text |
| a deadline or a network timeout | `timed out after <timeout>`, with `<timeout>` the timeout its caller gives |
| a cancellation | `cancelled` (`redact.Cancelled`) |
| an end of input | `connection closed` |
| any other error | its Go type and `, text withheld` |

**The operator file** (`--operator`, `evidence.LoadOperator`) names the provider, the account tier, the client host and one record of each required kind; `LoadOperator` hashes every record before the run. `evidence.CheckSource` refuses a build with no version-control revision, and a modified tree without a `source_diff` record.

On a release route with a confidential limit, `Operator.CheckConfidential` refuses an operator file whose path or text fields carry the value of a confidential limit, the configured one or the one the refresh command read (`Operator.Withhold`). The fields searched are the provider, client host, cell and roles, and each record's path, note and URL (Uniform Resource Locator). It searches each form `config.ValueForms` gives, skipping a form that lies within an identifier the provider fixes (`config.CarriesOutside` over `config.FixedIdentifiers`).

**The admission ledger** (`--ledger`, `internal/admission/ledger.go`) is one append-only JSON Lines (JSONL, one JavaScript Object Notation object per line) file. Each line carries its sequence number and the SHA-256 of every byte before it; `admission.Parse` checks the chain on every read and append, and a chain that does not check admits nothing. An admission reserves its requests at the route's highest recorded price for their token bounds, and is settled with the count sent.

The pilot's caps are `PilotCaps`: `PilotRouteRequestCap` (1,024), `PilotTotalRequestCap` (2,048) and `PilotLiabilityCapUSD` (US$40).

### A hosted run, in order

1. **Admission, status and clock.** A pilot window is admitted in the ledger, then `--status-cmd` and `--clock-sync` run; an offset above `PinnedClockOffsetMaxS` (60 s), or no reading, makes the window client-invalid. A release cell runs its clock, status, refresh and balance commands before its admission; there the offset stops the route and a missing reading blocks the cell ([A cell, in order](#a-cell-in-order)).
2. **Probe** (`probe.Run`, SPEC-HOSTED §5): DNS (Domain Name System) resolution, the TLS handshake and one request; a 400, 401, 403 or 404, or a status in `target.probe.account_limit_statuses`, blocks the start (`probe.Gate`).
3. **Load** through `loadgen.Run` with the canary; one case text per request through `Hooks.Cases`. A pilot window's tripwires (`newTripwire`) record a send beyond the schedule and a fifth request in flight (`PinnedPilotInFlightTripwire`, 4); a fired tripwire makes the window client-invalid without stopping it.
4. **After the last terminal event:** the end status, the frame store, `attempts.jsonl` and `manifest.json`. A release cell writes its report pair through `report.WriteHostedPair` before the manifest that names it.

### The pilot

`cmd/percentes-pilot` runs the pilot profile, whose constants are in `internal/config/config.go` and enforced in `internal/config/pilot.go`: two targets (`PinnedPilotTargets`), one request every 10 s (`PinnedPilotRateRPS`), a 60 s warm-up, a 1,200 s window, a 4,096-token ceiling and the 30 s timeout.

| Part | Code |
|---|---|
| Cases: 120 measured and 24 development synthetic tickets, labelled `billing`, `technical`, `account` or `escalation` | `pilot.Generate` (`internal/pilot/tickets.go`) |
| Scorer: one JSON object whose one key is `route`; ten task outcomes | `score.NewScorer`, `Scorer.Score` |
| `probe`, `dev` (the development gate), `session` | `cmd/percentes-pilot/probe.go`, `dev.go`, `session.go` |
| Session schedule from a master seed, re-derived and compared | `pilot.NewSchedule` |
| Paired pilot report | `report.GeneratePilot`; no command calls it |

### Replay and the completeness check

In the commands below, DIR is a cell's directory, FILE after `--config` the configuration as loaded, OUT an output directory other than DIR, whose report pair is overwritten, ROOT the `--frames` root and KEY the release key file.

| Command | Calls | Checks |
|---|---|---|
| `percentes --replay DIR --config FILE --out OUT` | `report.FromRun` | renders the pair from `manifest.json`, `attempts.jsonl` and the configuration, checks hashes, invalid reasons, G6, per-step G2 (`stepG2`), canary summaries (`stepCanary`) and the ceiling-rule verdict (`admission.CheckPublished`); exits 1 unless both files match the manifest byte for byte |
| `percentes --check-release DIR --config FILE --frames ROOT [--release-key KEY]` | `evidence.CheckRelease` | the cell's files and records against its manifest (list below), naming every problem it finds |

`evidence.CheckRelease` checks these:

- a build revision in the manifest, and a source diff named where the build's tree was modified;
- the recorded hash and size, or keyed hash (`readRef`), of each file the manifest names in DIR: `artifacts` and the clock, status and balance records, though not the ceiling-check record copies that the manifest's `release` record names in `probes`;
- for a release cell, the label against its gates, the ceiling-rule verdict (`admission.CheckPublished`), and each attempt's shape and ceiling, with its connection and write times in order after its dispatch (`recordProblems`);
- for a release cell, the dispatch series, gap histograms, canary, balance records, no answer text, and the expected and reported models;
- the same hashes of the files in the cell's frame directory under ROOT: the frame store, the operator file copy, the record copies and the source diff;
- where the configuration given differs from its published form, the retained `config.yaml` beside the frames, which must hold the configuration given byte for byte.

Both commands compare the manifest's configuration hash with that of `Config.Published` for the configuration given. Neither applies to a pilot window, whose manifest names no report pair.

### The mock's shaped form

`mock.NewShaped` serves a hosted provider's stream form for the hosted tests and the rehearsal; `cmd/mockserver` builds the Phase 0 form only. A `Shape` (`internal/mock/shape.go`) sets reasoning chunks, the answer in four-rune content chunks, the finish reason, the usage chunk, an in-band error, an unknown key, a cut body and the one credential header the key must arrive in. With `Messages` set it serves the Messages stream (`internal/mock/messages.go`).

## The release runner

`cmd/percentes-release` runs one route as one process (SPEC-HOSTED §1 to §9). A release's routes run concurrently on one client host at their own offsets, under one `--out` directory.

| Subcommand | What it does | Code | Exit codes |
|---|---|---|---|
| `key create`, `key sum` | write the release key; print each file's HMAC-SHA256 under it | `key.go`, `keyCmd` | 0; 1 |
| `ledger create`, `ledger price` | start the admission ledger with the routes' caps; record a route's prices for its cells and ceiling check | `ledger.go`, `ledgerCmd` | 0; 1 |
| `ceiling-check` | send a route's ceiling check and write its record; `--recover` settles unsettled checks, `--record FILE` installs a record printed on standard error | `ceiling.go`, `ceilingCheckCmd` | 0 the check and its input and output bounds passed and `run` accepts the reported model, or `--recover` settled or installed a record. 2 the check or a bound failed, its record's outcome is `refused` or `stopped`, its start gate blocked it, or it carries a model fault. 1 the command refused to start ([its table](#before-the-first-cell)), a record could not be written, or the ledger did not settle the admission |
| `schedule` | write the committed schedule of the given routes | `schedule.go`, `scheduleCmd` | 0; 1 |
| `public-config` | write a route's configuration as published | `public.go`, `publicConfigCmd` | 0; 1 |
| `run` | run one route's cells over the committed window | `route.go`, `runCmd` | 0 every cell counts; 4 a cell does not count; 3 the route stopped, or a stop is unacknowledged; 5 SIGTERM with no stop; 1 a refusal to start, or an execution error after it |
| `dispatch` | sum every cell's dispatch series under `--out` | `dispatch.go`, `dispatchCmd` | 0; 1 |
| `pages` | write the index page and each route's page; with `--cell-files`, each route page names each cell's archive | `pages.go`, `pagesCmd` | 0; 1 |
| `pack` | write a route's evidence pack, its pages built at the `--cell-files` address given | `pack.go`, `packCmd` | 0; 1 |
| `publish-check` | check the release publication record against the pages and packs it builds at the `--cell-files` address given; `--write-page` writes the publication page | `publish.go`, `publishCheckCmd` | 0 the record is complete and the publication page under `--pages` is the one it gives; 2 either is not; 1 |
| `rehearse` | run the five-step rehearsal against mock routes | `rehearse.go`, `rehearseCmd` | 0 green; 2 not; 1 |
| `check-cpu` | report whether this build measures host CPU | `main.go`, `checkCPUCmd` | 0 measured; 2 not |

Exit codes are the constants in `cmd/percentes-release/main.go` (`exitOK` to `exitTerminated`). Any command exits 2 on a Go panic or a fatal runtime error. SIGINT and SIGHUP (the hang-up signal) cancel the context of `ceiling-check`, `run` and `rehearse` with `loadgen.ErrOperatorAbort` (`operatorContext`); SIGTERM cancels it with `errTerminated` for `run` and as an operator abort for the other two. The other commands take no context, and the three signals do not stop them: each runs to completion.

Once started, `run` exits 1 on an execution error in `route.loop`. The next start does this:

| Execution error | Next start |
|---|---|
| an error inside a cell (`runCell`) | the cell is labelled `interrupted`, except a stopped cell, which keeps its label and makes `run` exit 3 (`cellEnded`), and a cell whose evidence was written (`retain`) and whose ledger settlement alone failed, which keeps its outcome; `route.state` resumes from the cell records |
| a failed label, wait or read of the cell records | `route.state` resumes from the cell records |
| a refresh fault at the window close (`windowClose`) | no window-close record was written, so the window close runs again |
| the ledger's open line changed (`readLedger`) | the cell is labelled `interrupted`; `checkLedger` refuses to start until the ledger the schedule froze is back |
| a cell record written under another schedule file (`scheduleOf`) | `sameSchedule` refuses to start |

### The release profile

`internal/config/release.go` refuses a release file outside these values:

| Field | Value | Constant or file |
|---|---|---|
| `release.ladder.step_fractions` | 0.25, 0.50, 0.75, 0.90 and 1.00 of the binding rate, the rate the binding-rate rule (`admission.Solve`) solves for the route | `PinnedReleaseStepFractions` |
| `release.ladder.warmup_s`, `step_s`, `drain_s` | 60 s, 240 s, 90 s; the drain follows each step's measured window, with nothing scheduled, while that step's requests end | `PinnedReleaseWarmupS`, `PinnedReleaseStepS`, `PinnedReleaseDrainS` |
| `release.shape` | Shape S: input bound 1,200, ceiling 512, fallback 1,024; Shape R: input bound 128, ceiling 16, fallback 256 where `fallback` is true; `deadline_s` 30 | `PinnedReleaseShapes`, `PinnedReleaseDeadlineS` |
| `client.http_timeout_s` | 90 s, the hard timeout | `PinnedReleaseHardTimeoutS` |
| `load` | `arrival_process` `deterministic`, `connections` 32 | `PinnedReleaseArrivalProcess`, `PinnedReleaseConnections` |
| `target.unpublished_limits` | each entry limits the route to 0.5 requests per second | `PinnedReleaseUnpublishedCapRPS` |
| `release.caps` | `route_usd` within `global_usd`, both positive decimals | `release.go` |
| `release.other_hosts`, `release.account_group` | the route's host group and account group, both defined in the [Glossary](#glossary) | `release.go` |

The literal body carries `stream` true, the case slot once and the ceiling key of the route's adapter (`target.adapter`, the streaming request form: chat completions or Messages), such as `max_tokens`, at `release.shape.ceiling_tokens`, with no sampling parameter. Each `target.limits` entry records a dimension, value, marking (`public` or `confidential`), source and hash, with `evaluation_windows_s`, `counting`, `output_at_generation` and `reset_zone` where they apply.

`percentes-release public-config` writes `Config.Published`: each confidential value replaced by `"[retained]"` and the rest of the file unchanged. It refuses a file carrying a confidential value elsewhere in a form `config.ValueForms` gives, outside the identifiers the provider fixes (`config.CarriesOutside`).

### The release key

`releasekey.Create` (`key create --key FILE`) writes `Size` (32) random bytes as lowercase hexadecimal to a new file with mode 0600. `releasekey.Read` refuses a file with any group or other permission bit, and one that is not 64 lowercase hex digits and a newline. A `releasekey.Key` prints as `[release key]` and `encoding/json` writes none of its bytes.

The schedule's `config_hmac_sha256` is keyed on every route; the other fields below are keyed only on a route with a confidential limit. Every other published hash is a plain SHA-256.

| Record | Keyed field |
|---|---|
| the schedule file (`--schedule`), every route | `config_hmac_sha256`, the configuration as loaded |
| configuration, ceiling-check records, `report.json`, route pages | `hmac_sha256` of a confidential limit's retained copy |
| `probe.json`, ceiling-check records | `hmac_sha256` of a withheld body; `error_hmac_sha256` |
| `attempts.jsonl` | `stream_error_hmac_sha256` |
| `manifest.json`, `window-close.json` | `hmac_sha256` of the frame store, operator file and each record, with no `bytes` |

`schedule` and `run` require `--release-key`; `ceiling-check`, `pages`, `pack`, `publish-check` and `percentes --check-release` require it where any route has a confidential limit; `rehearse` makes one in memory.

### The schedule

`schedule.New` derives the sampling frame from one master seed (SPEC-HOSTED §4); `schedule.Load` refuses a file whose bytes differ from what its own input derives.

| Rule | Value | Constant or function |
|---|---|---|
| Cells per route | 12: slots 00, 04, 08, 12, 16, 20 UTC for each day type, weekday and weekend; each cell's attempt 1 date is drawn from its day type's dates but the last, and the weekend cells' dates include a Saturday and a Sunday | `SlotHoursUTC`, `derive` |
| Window | at most 14 days | `MaxWindowDays` |
| Attempts per cell, re-runs per route | 2, 2 | `MaxAttempts`, `MaxReruns` |
| Offsets | a permutation of 0, 40, 80 ... minutes over the routes in name order, drawn from the seed | `OffsetStep` |
| Account-group separation | two routes sharing an account group have offsets no closer than 80 minutes, in either direction round the slot | `GroupSeparation` |
| Start tolerance | 15 minutes; a start or ladder epoch later than this labels the cell missed | `StartTolerance` |
| Cell length | 60 + 5 × (240 + 90) s, 28.5 minutes | `CellLength` |

`scheduleCmd` freezes per route (`configs`): `config_sha256` of the file as published (`frozenConfig`), `config_hmac_sha256`, `host_group`, `account_group`, and `binding_rps`, `binding_limit` and `binding_window_s` from `admission.Solve` (`freezeBinding`, `bindingSolution`); and for the ledger, `ledger_sha256` of its open line, `ledger_lines` and `ledger_head_sha256`. It reads each route's ceiling choice from `--ceiling-probe` records (`scheduleChoices`).

`schedule` exits 1 on each refusal below. Rows are in the order the checks run.

| `schedule` refuses when | Checked by |
|---|---|
| a flag does not parse; `--ledger` or `--schedule` is missing; no `--config` is given, or one does not load or is not under the `release` profile | `scheduleCmd`, `loadReleases` |
| `--release-key` is missing, or names a file `releasekey.Read` refuses | `readReleaseKey`, `releasekey.Read` |
| the ledger does not open, or its chain does not check | `admission.Open` |
| the ledger's global cap or a route's cap differs from that route's configuration | `State.CheckRelease` |
| a configuration has no published form | `frozenConfig`, `Config.Published` |
| the master seed, the window's dates or the route list derive no schedule | `schedule.New` |
| the ledger's open line, or its lines through the position first read, change before the command reads them again | `readLedger`, `frozenEntries` |
| a ceiling-check record does not read, is not the record `ceiling-check` writes, or was admitted in another ledger | `scheduleChecks`, `readCheckRecord` |
| a record names a ledger line that admits no ceiling check of its route up to the frozen line, or a line another record names; a ceiling-check admission there has no record | `checkAdmitted` |
| the ledger holds no price line for a route up to the frozen line, or a route's binding rate puts its lowest step at 0 requests per second | `bindingSolution`, `routePrice`, `freezeBinding` |
| a route's other host has no configuration given, sends another model, shape or ceilings, or names other hosts | `checkHostGroups` |
| the routes of one account group record different `spend_per_month_usd` limits, or their route caps sum above the least of those limits | `checkAccountGroups` |
| a route has no record of its host group; its records fail a record check `run` also makes, or pick a ceiling that differs from `release.shape.ceiling_tokens` and do not exclude the route | `scheduleChoices`, `ceilingChoiceFor` |
| `--excluded` does not name exactly the routes the records exclude | `checkExcluded` |
| the `--schedule` file already exists, or does not write | `scheduleCmd` |

### Before the first cell

Order: `key create`; `ledger create`; `ledger price` per route; `ceiling-check` per route at each ceiling checked; `public-config` per route; `schedule`.

The refusal tables below are those of `ceiling-check` and of `run`; `schedule`'s are in [its table](#the-schedule).

**The ceiling rule** (SPEC-HOSTED §3). Its binding-rate rule, `admission.Solve`, solves the binding rate at the chosen ceiling as the least of the rates in the table below (`rule.solve`), rounded down to a multiple of 0.001 requests per second. `admission.CeilingRule` applies the whole rule: it tests each ladder step at the frozen binding rate, and its verdict carries the binding-rate rule's solution.

Nominal counts are a cell's requests per unit rate: `warmup_s` × the first step fraction plus `step_s` × the sum of the fractions, 831 at the pinned ladder (`PinnedReleaseWarmupS`, `PinnedReleaseStepS`, `PinnedReleaseStepFractions`).

| Term | Rate it admits |
|---|---|
| public rate, token or spend-window limit | the requests it admits in each evaluation window (`evaluation_windows_s`), over W + 0.1 s, with W the window's length, adding the hard timeout to the divisor where marked `output_at_generation` |
| concurrency limit | the limit over the hard timeout |
| daily limit | for each reset day in its `reset_zone` that holds an attempt: the rate at which the day's attempts and its ceiling-check requests fit within the limit. Each attempt counts as one cell at nominal counts plus one probe request, and each check request counts at the highest ceiling the shape permits. The term is the least rate over those days (`rule.dailyRate`). An attempt's interval runs from `ReleaseStartTolerance` (15 minutes) before its scheduled start until that tolerance and one cell length after it. A day holds every first attempt whose interval touches it and at most `ReleaseReruns` (2) re-runs |
| spend per month, route cap | the rate at which `ReleaseCells` (12) cells and `ReleaseReruns` (2) re-runs at nominal counts, with `ReleaseProbeAllowance` (100) probe requests, cost no more than the limit (`rule.capRate`) |
| auto-reload threshold | the rate at which one cell at nominal counts costs no more than the threshold |
| `target.unpublished_limits` entry | 0.5 requests per second |
| confidential limit | sets no rate; `CeilingRule` tests each step against it at the frozen binding rate. A step it fails at the recorded value makes `run` refuse to start (`newRoute`). A step it fails at the value read at a cell's start stops the route (`limitsStop`), and the stop names the limit by its dimension, source and keyed hash (`bindingText`) |

`ExpectedPlan` gives the count rule: floor(r·T) requests in a T-second segment at rate r.

`CeilingRule` then tests each step on integer counts, over the cell up to that step: the largest warm-up count plus the largest count of each step through it, over every attempt in the frozen schedule (`exactPlan`). The cap and reload tests of a step use that cell. The daily test counts each of a reset day's attempts as that cell plus one probe request, and adds the day's check requests.

**The ceiling check** (`probe.RunRelease`, SPEC-HOSTED §2) sends ten texts of the route's shape at the configured ceiling, paced at the lowest step's arrival interval (`checkStepRPS`), admitted with `Ledger.AdmitCeilingCheck`. `ReleaseRecord.Outcome` classes a record `stopped` where the start gate blocked the check, `refused` where a request refused it (`RefusedBy`, table below), then `stopped` where it was interrupted or ended at a request that reached no connection, and `ran` otherwise.

`probe.ChooseCeilingAt` decides a host group's ceiling, leaving out a host whose records carry a model fault (`hostModelFault`); a model fault in the route's own records (`probe.ModelFault`) refuses or excludes the route.

The first request that meets a row below refuses the check, and the row's status form is what `RefusedBy` returns. Where a deciding record is refused, the ceiling choice excludes the host group.

The exclusion's reason holds, for each status form, the clause `<hosts>: ceiling check inconclusive, <status> at <ceiling>`, after any earlier ceiling's failures. `<hosts>` names the hosts so refused, `<status>` is the status form and `<ceiling>` the ceiling as `probe.Number` prints it, 1,024 for example. Where a host is left out on the reported model id, that clause comes first, followed by `; `.

| A request of the check | Status form |
|---|---|
| answered 429 or 500 to 599 | `HTTP <code>`, with `<code>` the status |
| reached a connection and got no status | `no response` |
| answered 200, with an in-band error whose type `target.error_docs` maps to 429 or 500 to 599 and that is not one of the route's documented billing refusals, which block the start instead (`CheckRequest.InBandStatus`, `probe.InBandRefusal`) | `HTTP <code> in-band` |
| answered 200, its stream ended on a read error before its terminator (`probe.StreamCut`) | `stream cut` (`probe.RefusedStreamCut`) |
| answered 200, its stream stopped short of its terminator without a read error or an in-band error (`probe.EndedEarly`) | `stream ended early` (`probe.RefusedEndedEarly`) |

The last three rows apply only to a stream with no undecodable or dropped event. The last request of an interrupted check, ended by its cancellation, refuses the check by none of `no response`, `stream cut` or `stream ended early`.

An in-band error of a type that `target.error_docs` does not map to 429 or 500 to 599 refuses nothing: it fails the check (`ceilingFail`), as an undecodable or dropped event does. A documented billing refusal, by status or in-band, blocks the start, and the record is `stopped`.

`ceiling-check` refuses to start, exiting 1 before it admits the check, on each row below. Rows are in the order the checks run.

| `ceiling-check` refuses when | Checked by |
|---|---|
| a flag does not parse; `--config`, `--ledger` or `--out` is missing; `--record` is given without `--recover`; the configuration does not load or is not under the `release` profile | `ceilingCheckCmd`, `loadReleases` |
| a limit's source is a file and `--snapshots` is missing; the configuration withholds probe values and `--frames` is missing; `--frames` is inside the source tree | `ceilingCheckCmd`, `probe.Retains`, `evidence.CheckFramesRoot` |
| on a route with a confidential limit, `--release-key` is missing or names a file `releasekey.Read` refuses | `readReleaseKey` |
| `--byte-cap` is missing on a Shape S route, is too small to hold a Shape S ticket's header and message, or is given on any other | `routeShape`, `pilot.NewShape` |
| the ledger does not open, its chain does not check, or its caps differ from the configuration's | `admission.Open`, `State.CheckRelease` |
| the ledger's open line changes before the command reads it again | `ledgerEntries`, `readLedger` |
| a record under `--out` does not read or decode, or two name one ledger line | `ledgerChecks` |
| a cell of any route is already admitted in the ledger | `runAdmitted` |
| `--out` lacks the record of one of the route's ceiling-check admissions | `checkPriorRecords` |
| an earlier record of the route does not re-derive under the configuration as it now stands | `ReleaseRecord.Verify`, `frozenVerify` |
| the ledger holds no price line for the route, or the rate its limits admit puts the lowest step at 0 requests per second | `routePrice`, `checkStepRPS` |
| on Shape S, a record that was not stopped is at a byte cap other than `--byte-cap` that no check spent | `releaseCapCheck` |
| on Shape S, `--byte-cap` is at or above a spent byte cap | `repeatCheck` |
| the ceiling is the fallback, and either the route has no deciding record (`probe.Decisive`) at the first ceiling, or the host group's checks there do not choose the fallback with the route still in | `fallbackCheck`, `probe.ChooseCeilingAt` |
| an earlier check of the route at this ceiling decides, or a refused one's quiet interval has not passed | `repeatCheck`, `probe.RepeatQuiet` |
| `--env` is missing; the env file does not read, lets its group or other users read it, holds a line that is not `NAME=value` or a name set twice, or does not set the target's `api_key_env` | `loadKey`, `readEnvFile` |
| another process holds the route's lock on the ledger | `Ledger.LockRoute` |
| a retained limit copy in `--snapshots` is missing or does not match its recorded hash | `probe.CheckSnapshots` |
| an operator abort came before the admission | `sendCeiling` |
| the ledger's chain no longer checks, or its open line is not the one the command first read | `Ledger.AdmitCeilingCheck` |
| the route, or its account group, has a ceiling check the ledger has not settled | `Ledger.AdmitCeilingCheck` |
| the ledger holds no price at the check's token bounds, or the check's ten requests would take the route's or the release's projected liability over its cap; the ledger records a refused line | `Ledger.AdmitCeilingCheck` |

Once the check is admitted, `ceiling-check` exits 2 on each outcome [the subcommand table](#the-release-runner) lists (`checkOutcome`). It exits 1 where the record, or what it withholds under `--frames`, does not write (`writeCheck`), or the ledger does not settle the admission (`sendCeiling`); a record that did not write goes to standard error.

With `--recover` it refuses, sending nothing, where another process holds the route's lock, the ceiling or byte cap on an unsettled admission's line differs from `--config` and `--byte-cap`, or no check of the route is unsettled (`recoverChecks`). With `--recover`, the table's rows through `ledgerChecks` apply and the later rows do not; it also exits 1 where the ledger's state or an interrupted record does not read.

With `--record` it refuses, in this order, a record that does not read or decode, a route whose lock another process holds, a record of another route, ledger or shape, one whose line admits no ceiling check of the route or already has a record, one at another ceiling or byte cap, and one `ReleaseRecord.Verify` refuses (`installRecord`). Each refusal exits 1, as does a record write or a settlement that fails in either mode.

`run` exits 1 on each refusal below unless the row gives another exit code. Rows are in the order the checks run, up to the call of `runCell` that admits the first cell.

| `run` refuses to start when | Checked by |
|---|---|
| a flag does not parse; `--config`, `--ledger`, `--schedule`, `--out`, `--frames`, `--operator` or `--account-created` is missing | `runCmd` |
| the configuration or a `--host-config` file does not load | `config.LoadFile` |
| `--release-key` is missing, names a file whose mode gives its group or other users any access, or names one that is not 64 (2 × `releasekey.Size`) lowercase hex digits and a newline | `readReleaseKey`, `releasekey.Read` |
| `--ack-dir` is missing, does not exist, is not a directory, lies under `--out` with the symbolic links of both resolved, is writable by the route's process, or is not readable and searchable by it | `runCmd`, `checkAckDir`, `resolvedPath` |
| the profile is not `release` | `newRoute` |
| the schedule file does not read, or is not the schedule its own input derives | `schedule.Load` |
| the schedule gives the route no offset | `Schedule.Runs` |
| the schedule freezes no configuration of the route, or froze another host group, another SHA-256 of the configuration as published, no HMAC-SHA256 of the configuration as loaded, or another one under the `--release-key` key; or the configuration has no published form | `checkFrozen`, `frozenConfig`, `Config.Published` |
| a `--host-config` file is not under the `release` profile, is of a route `release.other_hosts` does not list or of one already given, sends another shape or other ceilings, or is not the configuration the schedule froze for its route; a route in `release.other_hosts` has no `--host-config` | `hostTargets`, `sameShape`, `checkFrozen` |
| a `cell.json` under the route's directory in `--out` does not read or decode, or its `schedule_sha256` is not the SHA-256 of the `--schedule` file | `sameSchedule`, `scheduleOf` |
| the operator file does not load | `evidence.LoadOperator` |
| where the route has a confidential limit, a cell manifest under the route's directory, or the refresh record as run kept beside its frames, does not read | `retainedReads` |
| the operator file holds a confidential limit's value in a text it publishes | `Operator.CheckConfidential` |
| the binary carries no version-control revision, or was built from a modified tree and the operator file has no `source_diff` record | `evidence.CheckSource` |
| the open-file soft limit is below `minOpenFiles` (65,536) | `checkOpenFiles` |
| `--frames` is inside the instrument's source tree | `evidence.CheckFramesRoot` |
| `--clock-sync` is missing, or is not `sntp` with a server or `chronyc` ending in `tracking` without `-c` | `evidence.ParseClockCommand` |
| `--status-cmd`, `--balance-cmd` or `--refresh-cmd` is missing | `evidence.ParseStatusCommand`, `ParseBalanceCommand`, `ParseRefreshCommand` |
| the ledger does not open, or its chain does not check | `admission.Open` |
| the schedule freezes no ledger, or the ledger's open line has another SHA-256 than the one the schedule froze | `checkLedger` |
| the ledger's global cap or the route's cap differs from `release.caps.global_usd` or `release.caps.route_usd` | `State.CheckRelease` |
| the ledger's open line changes before the command reads it again, or a line does not decode | `readLedger` |
| the schedule freezes no ledger position, the ledger holds fewer lines than the frozen count, or its bytes through that line have another SHA-256 | `frozenEntries` |
| the ledger holds no price line for the route up to the frozen position | `routePrice` |
| the schedule freezes no binding rate of the route | `newRoute` |
| the binding-rate rule at the route's newest price gives another binding rate, binding limit or window than the schedule froze | `bindingSolution`, `admission.Solve` |
| the configuration builds no release ladder at its binding rate | `exactPlan` |
| the ceiling rule refuses its inputs | `admission.CeilingRule` |
| the ledger, read again, no longer checks or has another open line | `Ledger.State` |
| the ledger holds no price at the cell token bounds, or one below the ceiling rule's price per request | `State.Covers`, `admission.ReleaseTokens` |
| the ceiling rule at the frozen binding rate refuses a step at the schedule's counts, against any limit, a confidential one among them | `newRoute`, `admission.CeilingRule` |
| `--byte-cap` is missing on a Shape S route, is too small to hold a Shape S ticket's header and message, or is given on any other | `routeShape`, `pilot.NewShape` |
| no `--ceiling-probe` record is given | `ceilingChoiceFor` |
| a host's `target.body` gives no request body at a ceiling the shape permits | `config.ReleaseBodyAt` |
| a record does not read or decode, holds a field `probe.ReleaseRecord` does not define, or has bytes other than the `probe.Encode` encoding of the record it decodes to | `ceilingChoiceFor`, `probe.Encode` |
| a record is of a route outside the host group, unless the route is Shape S and the record spent a byte cap | `ceilingChoiceFor`, `probe.Spent` |
| a record of a route outside the host group that spent a byte cap names another ledger, or its stored results differ from the ones its requests give | `ceilingChoiceFor`, `ReleaseRecord.Verify` |
| a host's record carries another requested model, shape, ledger open-line SHA-256, expected model identifier, adapter or endpoint than its host's configuration gives, or another SHA-256 of `target.body` at the record's ceiling | `ceilingChoiceFor` |
| a host's record carries a value its host's configuration withholds, or another host's record carries a value its own confidential markings withhold | `probe.Exposed`, `probe.ExposedByMarking` |
| a host's record holds stored results other than the ones its requests give under its host's configuration | `ReleaseRecord.Verify`, `frozenVerify` |
| two records of one route, ceiling and ledger line are given | `ceilingChoiceFor`, `probeCopyName` |
| `--byte-cap` is above a byte cap a record spent | `probe.Spent` |
| a host's record that was not stopped carries a byte cap other than `--byte-cap` that is not a spent cap above it | `ceilingChoiceFor` |
| a response in the route's own records names a model other than the expected model, or no response names the recorded `target.reported_model` | `probe.ModelFault` |
| a host's repeat of a refused check at one ceiling started less than `probe.RepeatQuiet` after the refused check ended: the host's longest limit window plus `config.PinnedReleaseHardTimeoutS` (90 s) | `repeatQuiet`, `probe.RepeatQuiet` |
| a record names a ledger line that admits no ceiling check of its route up to the frozen line (the last line the schedule read), or a line another record names; a ceiling-check admission of a host up to the frozen line has no record | `checkAdmitted` |
| a record of a host left in the choice is at a ceiling the shape does not permit | `probe.ChooseCeilingAt` |
| at any ceiling, a host left in the choice has more than two records that were not stopped or superseded, two that name one ledger line, or two of which the earlier was not refused | `probe.ChooseCeilingAt`, `probe.Decisive` |
| at a ceiling the rule reaches, a host left in the choice has no deciding record: no record there that was neither stopped nor at a superseded byte cap (a spent cap above `--byte-cap`). Where the latest superseded record there had a prompt reported over the input bound, the rule excludes the host group instead | `probe.ChooseCeilingAt` |
| the records exclude the host group, or choose a ceiling other than `release.shape.ceiling_tokens` | `runsAt` |
| a ceiling check of the route's account group was admitted after the frozen ledger line | `lateChecks` |
| `--account-created` is not a date written YYYY-MM-DD (year, month, day) | `newRoute` |
| `--env` is missing; the env file does not read, lets its group or other users read it, holds a line that is not `NAME=value` or a name set twice, or does not set the target's `api_key_env` | `loadKey`, `readEnvFile` |
| another process holds the route's lock on the ledger, `<ledger>.<route>.lock`, where `<ledger>` is the `--ledger` path | `Ledger.LockRoute` |
| a cell record under the route's directory does not decode or is of another route; a cell record left running has a stop marker or probe record that does not read, or, with no end recorded, a `dispatch.json` or `attempts.jsonl` that does not decode; such a record, once labelled, does not write | `route.state`, `recordsEnd` |
| an abort or stop marker under the route's directory does not read or decode | `route.holds` |
| a stopped cell with no stop marker, an operator abort marker or a stop marker lacks its acknowledgement file in `--ack-dir`, `resume-<route>-<id>`, with `<id>` as [A cell, in order](#a-cell-in-order) gives it; `run` prints the file that releases each hold (exit 3) | `route.holds`, `route.ackFile` |
| SIGTERM comes before a label or the window close, during the window close, or during the wait for the next run (exit 5) | `route.endTerminated` |
| a cell that `route.state` finds unlabelled does not label | `labelCell` |
| no run is left: an operator abort stops the route before the window close (exit 3), or the window close fails | `route.windowClose` |
| a run whose start passed more than `StartTolerance` ago has a `missed` record that does not write | `route.writeNew` |
| an operator abort comes during the wait for the next run, and `run` writes an abort marker (exit 3); the wait ends on any other error (exit 1) | `abortHold.write`, `waitUntil` |

`pages` reads the ceiling-check records too, and [its table](#the-per-step-tables-and-the-index) lists its refusals.

**Shapes.** `pilot.NewShape` builds each shape's text for the check and the load. Shape S (`pilot.GenerateShapeS`): user text of the prefix `cs-<seed>-<index>`, where `<seed>` is the attempt seed and `<index>` the request's six-digit index, then a space and a synthetic ticket cut at the byte cap. Shape R (`pilot.GenerateShapeR`): the same prefix and one frozen instruction.

### A cell, in order

`route.loop` (`cmd/percentes-release/route.go`) runs once the start checks pass:

1. Take the route's lock on the ledger (`Ledger.LockRoute`).
2. Read the cell records (`route.state`), recovering the label of a cell left running; exit 3 while a hold has no acknowledgement file (`route.holds`).
3. Label each unlabelled cell, writing its labelled manifest and report pair from what its directory holds (`labelCell`).
4. Record as `missed`, with no command run, a run whose start passed more than `StartTolerance` (15 minutes) ago.
5. Wait for the next run's start; an operator abort in the wait writes an abort marker and exits 3.
6. Run the cell (`runCell`), then go back to step 2. After the last run, write the window-close record and exit (`route.windowClose`, `route.finish`).

`runCell` (`cmd/percentes-release/cell.go`):

1. Write `cell.json` as running; start the live dispatch series (`startLiveSeries`).
2. Read and write the clock record (`evidence.ReadClock`) and the start status.
3. Run `--refresh-cmd`, a route's `refresh-<route>` wrapper, which a release's freeze commit adds under `deploy/release/`; parse its limits (`evidence.ReadRefreshedLimits`); read the operator file again (`cellOperator`); write `refresh.json`.
4. Read the ledger bound to its frozen open line (`readLedger`). Unless the operator or refresh records failed, test the frozen step rates against the limits read (`limitsStop`, `cmd/percentes-release/close.go`), which also returns each window's write allowance (`admission.WindowAllowances`). A limit the command reports `unavailable` is tested at its recorded value.
5. Compute the reservation (`State.ReservationUSD`), run `--balance-cmd` and write `balance-start.json` with its check (`evidence.CheckBalance`).
6. Decide, taking the first of these that holds:
   1. a clock offset above 60 s stops the route;
   2. failing limits stop the route;
   3. SIGTERM interrupts the cell;
   4. an operator abort stops the cell (`stopped before admission: operator abort`);
   5. an operator or refresh fault blocks the cell;
   6. the balance check blocks the cell;
   7. a clock with no reading blocks the cell.
7. Admit one probe plus every scheduled request (`Ledger.AdmitAt`) at the route's price revision (`State.PriceRevision`). A `PriceChanged` repeats steps 4 to 6 once; a second blocks the cell. A cap refusal stops the route.
8. Send one probe (`probe.Run`). A documented billing refusal stops the route (`probeBilling`); a start-gate status blocks the cell, unless `authFault` finds a 401 or 403 the previous probed cell also saw, which stops the route. Where the cell then skips the ladder, a probe above the input bound or the ceiling stops the route (`pricingBreach`).
9. Wait one arrival interval at the first step's rate (`waitUntil`). A start more than `StartTolerance` late, or an epoch past `Hooks.LatestEpoch` (`ErrLateEpoch`), labels the cell missed (`probeSentNote`).
10. Run the ladder through `loadgen.Run` with the canary. `Hooks.OnDone` stops the route on a documented billing refusal (`billingRefusal`) and ends the ladder on client exhaustion (`clientExhausted`).
11. Count 401 and 403 responses (`countAuth`); a prompt above the input bound or output above the ceiling stops the route (`pricingBreach`).
12. Decide the outcome (`cellOutcome`) and retain the evidence (`retain`, `cellEvidence`): end status and balance, manifest, report pair.

The refresh command (`pct-refresh.py`, which a release's freeze commit adds under `deploy/release/`) prints one JSON object, `{"limits": [...]}`, with one entry per `target.limits` entry in its order; the balance command prints `{"amount_usd": "<decimal>"}`, with `<decimal>` the balance in US dollars as a decimal number, or `{"unavailable": "<reason>"}`, with `<reason>` its text. Each command runs for at most 30 s (`statusTimeout`; `clockTimeout` for the clock-sync command).

| Outcome | Meaning |
|---|---|
| `counted` | the ladder reached the top step's drain or the backoff rule; G2 passed over the ladder and at every attempted step, no tripwire, no exhaustion, clock unflagged, canary started, HTTP/1.1 (Hypertext Transfer Protocol 1.1) only, no model mismatch; enters the sampling frame |
| `client_invalid` | exhaustion, a failed G2, a tripwire, another protocol, a flagged clock, a run error, or no canary |
| `model_mismatch` | a response reported another model |
| `blocked` | a refresh, balance, clock-reading, repeated-price or probe-status block before the ladder |
| `stopped` | a route stop, or an operator abort before the outcome was decided; `run` exits 3 |
| `interrupted` | the process ended, SIGTERM came, or an execution error ended the cell before the outcome was decided; the next start labels it (`labelCell`) |
| `missed` | the start or the ladder epoch was more than 15 minutes late |

The labels are `CellCounted` to `CellInterrupted` in `internal/evidence/cell.go`. Every outcome but `counted` takes the re-run: each attempt 1 that did not count is re-run on its attempt 2 date while the route has fewer than two re-runs. The cell's JSON records go through `evidence.ReplaceFile`, so a crash leaves the old file or the new one.

While a stopped cell, an abort marker (`<route>/abort-<UTC time>.json`) or a stop marker (`<route>/stop-<label>-a<attempt>.json`) has no acknowledgement file under `--ack-dir`, every start exits 3 (`route.holds`, `cmd/percentes-release/hold.go`). `<label>` is the cell label, such as weekday-04, and `<attempt>` is 1 or 2. The acknowledgement file is `resume-<route>-<id>` (`route.ackFile`): `<id>` is `<label>-a<attempt>` for a stopped cell, and the marker's name without `.json` for a marker.

After the last run, `route.windowClose` runs the refresh command once more, compares the records with the last counted cell (`report.CompareWindowClose`) and writes `<route>/window-close.json`; `route.finish` then exits 0 or 4.

### The ladder

`loadgen.BuildLadder` places the warm-up at the first step's rate, then per step a measured window and a drain with nothing scheduled. The warm-up and each measured window take their counts from the count rule under [Before the first cell](#before-the-first-cell), and request i of a segment, counted from 0, goes out i/r seconds after the segment starts. `gen.runLadder` takes each step's verdict at its drain's end (`stepEnd`); later steps are recorded as not attempted. A stop leaves every undispatched request unsent.

Each release connection is an `observedConn` (`instrumentWrites`). A write holds a permit of `newWriteWindows` from its first byte to its last and counts in each window, whose width is the window's length plus `writeMarginNs` (50 ms).

A write that some window does not admit waits for a permit for at most `writePermitWaitNs` (`PinnedSendSkewMaxMs`, 50 ms) from its first attempt (`tryReserve`). It is refused at once where the next permit would come after that bound, and on waking where it woke after the bound, with a permit free or not.

The transport's idle pool holds 2 × the top step's rate × the hard timeout, plus `load.connections` (`Config.ReleaseConnectionProvision`).

### The cell's files and report

| File in `<route>/<label>-a<attempt>/` | Content |
|---|---|
| `cell.json` | the runner's working record (`cellRecord`, `cmd/percentes-release/route.go`): run, offset, `schedule_sha256`, outcome, reasons, rates, probe status, `auth_statuses`, ladder, tripwires, canary, concurrent runs, requests sent |
| `clock.json`, `status-start.json`, `refresh.json`, `balance-start.json`, `probe.json`, `status-end.json`, `balance-end.json` | the cell's records |
| `attempts.jsonl` | the release fields listed under the evidence files |
| `dispatch.json` | the cell's dispatches per UTC second, rewritten every second (`newDispatchSeries`, `liveSeries`) |
| `ceiling-probe-<route>-<ceiling>-<ledger line>.json` | one copy per ceiling-check record of the host group and, for Shape S, of each other route's record that spent a byte cap; `<ceiling>` is the record's ceiling and `<ledger line>` its admission's line |
| `manifest.json` | with the `release` record (`evidence.Cell`): `schedule_sha256`, `ladder_started`, `records_complete`, `billing_refusals`, `dispatch_aggregate` (`hostDispatch`), `inter_chunk_gaps` (`evidence.StepGaps`, 24 bins with lower edges `GapEdgesUs`), `canary`, `admission`, `ceiling_choice`; and `gates`: G2 over the ladder unless a stop cut it, then G2 and Hosted-G6 per attempted step (`releaseGates`) |
| `report.json`, `report.txt` | the cell report (`CellReport`), rendered from the manifest, the attempts file and the configuration |

`percentes-release dispatch` sums every `dispatch.json` into `dispatch-aggregate.json`.

### The per-step tables and the index

`report.NewStepTables` builds a route page from the ceiling-rule verdict and the cell records (`internal/report/steps.go`); `publish-check` builds each page again from the same records, at its own `--cell-files` address, and refuses a page under `--pages` with other bytes, naming the page.

| Part of a step | Content | Code |
|---|---|---|
| Main lines | `completed_in_time`, `completed_late`, `status_429`, `status_other`, `connect`, `reset`, `empty_stream`, `malformed_stream`, `censored`, `unsent`; each over scheduled with its 95 percent Wilson score interval (a binomial confidence interval) | `stepLines`, `stepTables`, `newRate` |
| Billing line | `billing_refusal`, printed last and only where a response was one; it is not pooled and has no index column | `lines` |
| Sub-lines | per status code, per in-band error type, stopped at the ceiling, unknown termination, per finish reason | `finishLabel` |
| G2, canary | cell report only, from the manifest | `stepG2`, `stepCanary` |
| Hosted-G6, throughput | in-time over scheduled against `PinnedBaselineGoodputMin` (0.99), and over the requests that were not `connect` or `reset` errors; completions per second over `release.ladder.step_s` | `cellTable`, `pool` (G6); `throughput` |
| Incidence curve and quantiles | Wilson band; quantiles in the SPEC.md §3 form, refused beyond the hard timeout | `curve`, `incidence` |
| Distributions | failure timing, TTFT, whole answer, by connection reuse, content events, tokens, prompt tokens, gap histogram | `signals`, `distribution`, `promptTokens` |
| `status_429` records | 30 s bins and each refusal's rate-limit and `Retry-After` headers and documented meaning (`status_429`, `warmup_status_429`, `dispatch_to_write`) | `segment`, `refusalDoc` |

| Route-page addition | Content | Code |
|---|---|---|
| Attribution | each `status_429` labelled at or below, above, or unresolved against each public limit's share from its account group's sends | `StepTables.Attribute`, `cellLimits`; history in `readHistory`, `reconcile` (`cmd/percentes-release/history.go`) |
| Cells outside the sampling frame | their `status_429` records, in no pooled figure | `StepTables.AddOutside` |
| Null reading | over a complete sampling frame, no `status_429` or 5xx (a status of 500 to 599) at the two top steps (0.90 and 1.00 of the binding rate), with Wilson upper bounds | `null`, `nullText` (`internal/report/null.go`) |
| Pooled lines | twelve counted cells; Wilson interval on the pooled scheduled count divided by DEFF (design effect) | `pool`, `designEffect` |
| Drift split | a change of tier, limits or model documentation between cells splits the sampling frame (`drift_parts`) | `DocsOf`, `drift` |
| Mismatch and window close | one line per `model_mismatch` cell (`model_mismatch_cells`); the window-close comparison (`window_close`) | read in `readRouteCells`, compared by `report.CompareWindowClose` and set in `buildPages`, printed by `StepTables.Text` |
| Cell archives | with `--cell-files`, each cell with a manifest and its archive's address, `<address>/<route>-<cell>.tar.gz`, `<cell>` being the cell's directory name `<label>-a<attempt>` (`cell_files`), printed on a `cell files:` line in the cell's section, or in a section of its own, `-- Cell <cell>, <outcome>, outside the frame --`, where no other section names the cell | read in `readRouteCells`, set in `buildPages` (`cellArchives`), printed by `StepTables.Text` |

`report.NewIndex` builds one row per route and step, ordered by provider, route and step, with each main line's pooled count divided by the pooled scheduled count; a row of an incomplete or drift-split sampling frame carries no counts. It refuses a string carrying any of `IndexForbidden` (`rank`, `winner`, `best`, `worst`, `interval`, `sort key`, `p99`). Excluded routes, routes that ran no cell and routes with stops and no manifest are listed under their own headings (`Index.Exclude`, `Index.AddUnrun`, `Index.AddStopped`).

`percentes-release pages` (`buildPages`) runs `evidence.CheckRelease` on every manifest, verifies each window-close record (`WindowClose.Verify`), checks the schedule's SHA-256 on every record (`readRouteCells`) and one ceiling choice per host group (`sameCeiling`), and writes `index.md`, `index.json`, `routes/<route>.txt` and `routes/<route>.json` only when every route reads. `excludedRoute` and `stoppedRoute` build the pages of routes with no manifest; `stoppedChecks` copies a stopped route's host-group ceiling-check records under `--out`.

`pages` gives a route that ran no cell a page read from its ceiling-check records alone (`excludedRoute`). Where those records exclude nothing, the index lists the route as unrun, with the ceiling check's choice as the reason. `pages` takes the release's byte cap (`releaseByteCap`) as the smallest cap among the Shape S records that were not stopped, or among all of them where every one was stopped.

`pages` exits 1 on each refusal below; on every row but the last it writes no page. Rows are in the order the checks run.

| `pages` refuses when | Checked by |
|---|---|
| a flag does not parse, or `--out`, `--frames`, `--pages`, `--schedule` or `--ledger` is missing | `pagesCmd` |
| `--cell-files` is not an https URL with a host and no user, query or fragment, written as `url.URL.String` writes it, or ends in a slash | `checkCellFiles` |
| no `--config` is given, or one does not load or is not under the `release` profile | `loadReleases` |
| `--release-key` is missing where a route has a confidential limit, or names a file `releasekey.Read` refuses | `checkingKey`, `releasekey.Read` |
| the schedule file does not read, or is not the schedule its own input derives | `loadSchedule`, `schedule.Load` |
| a cell record does not read or decode, is still running, or names another route or cell than its directory; a route's cells ran under different schedule files | `readRouteCells` |
| an interrupted cell with no end recorded has a `dispatch.json` or `attempts.jsonl` that does not read or decode, or a clock, status, balance or refresh record that exists and does not read | `cellEnd`, `recordsEnd` |
| a cell with no `manifest.json` is neither missed, interrupted, nor stopped with an execution error | `readRouteCells` |
| a cell manifest fails the completeness check ([its list](#replay-and-the-completeness-check)) | `evidence.CheckRelease` |
| a manifest names another run or schedule SHA-256 than its cell record | `readRouteCells` |
| a route's manifests carry different ceiling-rule verdicts, ceiling choices or probe records | `readRouteCells` |
| a cell its record counts is not counted in its manifest, or one scheduled cell is counted twice | `readRouteCells` |
| a cell's `attempts.jsonl` or `refresh.json` is not the file its manifest names, or does not decode | `readAttempts`, `readRefreshed` |
| a `--config` route outside the schedule has no cell record under `--out` | `readRouteCells` |
| a route's `window-close.json` does not read, or does not verify against the latest end of the route's cells | `evidence.ReadWindowClose`, `WindowClose.Verify` |
| a route's cells ran under a schedule file other than `--schedule` | `buildPages` |
| a route has cells under `--out` and no `--config` | `buildPages` |
| two hosts of one host group send different shapes or ceilings, ran at different ceilings, or carry different ceiling choices or probe records in their manifests | `sameCeiling`, `sameShape` |
| the ledger does not read, its chain does not check, its open line differs from the frozen one, or a line does not decode | `readLedger` |
| the schedule freezes no ledger position, the ledger holds fewer lines than the frozen count, or its bytes through that line have another SHA-256 | `frozenEntries` |
| a route of the schedule whose ceiling check the ledger admits up to the frozen line has no `--config` | `buildPages` |
| a `--check-record` does not read or decode, was admitted in another ledger than the schedule froze, or is of a route outside the schedule | `checkRecords` |
| a ceiling-check admission on the ledger after the frozen line names a route's account group or one of the group's routes | `lateChecks` |
| a ceiling-check record a manifest carries, or a `--check-record` file, does not read | `releaseCheckPaths` |
| one of those records is not the record `ceiling-check` writes | `releaseByteCap`, `readCheckRecord` |
| a route with stops and no manifest has a ceiling-check admission of its host group, up to the frozen line, that no given record names | `unnamedChecks` |
| a scheduled route with no cell has no given record of its host group | `excludedRoute`, `choiceFiles` |
| for such a route, its host group's records meet a refusal `ceilingChoiceFor` makes in the `run` table. Among them are a byte cap above a spent cap, a host's record that was not stopped at a byte cap neither the release's nor superseded, and a repeat begun inside the quiet interval | `excludedRoute`, `ceilingChoiceFor`, `repeatQuiet` |
| such a route's records exclude nothing, and the ledger admits a cell of the route after the frozen line | `excludedRoute` |
| among the records the manifests carry and the `--check-record` files, a ceiling-check admission of a scheduled route up to the frozen line has no record, two records name one line, or a record names a line that admits no check of its route | `allChecks`, `checkAdmitted` |
| no route has a page; two `--config` files name one route; a route's verdict and counted cells give no step tables (`report.NewStepTables`); an index field is empty, carries a table delimiter or a line break, or holds a string of `IndexForbidden` | `report.NewIndex`, `Index.Exclude`, `Index.AddUnrun`, `Index.AddStopped` |
| a route with a stopped cell and no cell manifest has no record of its host group, or its host group's records meet one of those `ceilingChoiceFor` refusals or carry a model fault in the route's own records | `stoppedChecks`, `ceilingChoiceFor` |
| for the account group's history, a cell's `probe.json` or `dispatch.json` does not decode, or a ceiling-check record names no request shape | `readHistory`, `cellSends`, `checkHistory` |
| a cell outside the sampling frame, or the account group's history, does not fit the route's step tables | `StepTables.AddOutside`, `StepTables.Attribute` |
| a page or a record copy does not write | `pagesCmd`, `replaceFile` |

`percentes-release pack` writes a tar archive of a route's page files under `pages/`, its cell manifests under `out/` and, where the route has no cell manifest, the copies of ceiling-check records that `pages` made for it, with `inventory.json` naming each by SHA-256 (`evidence.BuildPack`). Entries have fixed mode, owner and time, so a rebuild from the same cells is identical. `pack` builds the route's pages at its `--cell-files` address and refuses the addresses `pages` refuses. It refuses a route with a page and no window-close record.

`percentes-release publish-check` (`publishCheckCmd`) rebuilds the pages and packs and checks the publication record (SPEC-HOSTED §9, publication steps) with `Publication.Check` (`internal/evidence/publication.go`). Each route's entry in the record's `routes` holds its notice (`evidence.Notice`): the date sent, the recipient, the provider's account identifier, the evidence pack sent and the date the notice's window closes. An AWS (Amazon Web Services) route's entry also records the replication pack sent (`replication_pack`).

`publish-check` exits 1 on a refusal before the record is checked, and 2 on a refusal of the record; the Exit column gives each row's code. `Publication.Check` names every problem it finds at once; the copy and page checks in the last four rows run only once it passes. A release whose ceiling checks excluded every route has no cell manifest, and `publish-check` passes it once its record, pages, packs and ceiling-check record copies check.

| `publish-check` refuses when | Checked by | Exit |
|---|---|---|
| a flag does not parse, or `--out`, `--frames`, `--pages`, `--record`, `--schedule` or `--ledger` is missing | `publishCheckCmd` | 1 |
| `--cell-files` is one `pages` refuses | `checkCellFiles` | 1 |
| no `--config` is given, or one does not load or is not under the `release` profile | `loadReleases` | 1 |
| `--release-key` is missing where a route has a confidential limit, or names a file `releasekey.Read` refuses | `checkingKey`, `releasekey.Read` | 1 |
| the schedule file does not read, or is not the schedule its own input derives | `loadSchedule`, `schedule.Load` | 1 |
| the record does not read, holds a field `evidence.Publication` does not define, or has data after the JSON object | `evidence.LoadPublication` | 1 |
| the cells, the ledger or the `--check-record` files meet a refusal of the `pages` table above | `buildPages` | 1 |
| a cell manifest under `--out` does not read | `releaseManifests` | 1 |
| a window-close record under `--out` does not read | `releaseFiles` | 1 |
| a route's evidence pack does not build | `routePacks` | 1 |
| where a route has a confidential limit, one of its manifests, its window-close record or a refresh record as run kept under `--frames` does not read | `withheldValues`, `retainedReads`, `windowCloseReads` | 1 |
| `manifests`, `window_closes` or `pages` names a file twice, names one the release does not hold, records another SHA-256, or leaves one out | `Publication.fileSet` | 2 |
| a page the cells give at the `--cell-files` address is missing under `--pages`, does not read, or has other bytes; the page is named | `Publication.Check` | 2 |
| a date in the record, `non_working_days` included, is not written YYYY-MM-DD | `Publication.Check` | 2 |
| a `routes` entry is not a route of the release or is recorded twice; a route of the release is not in the record | `Publication.Check` | 2 |
| a route whose cells give a page has no `<route>/window-close.json` under `--out` | `publishCheckCmd`, `Publication.Check` | 2 |
| a route has no notice, or the notice's `recipient` or `account` is blank | `Publication.Check` | 2 |
| the release has no evidence pack for the route, or the notice's pack is not the pack `percentes-release pack` builds from the cells | `routePacks`, `Publication.Check` | 2 |
| a notice is dated before the UTC date of the route's latest cell start or of its window-close record, or a manifest or window-close record of the route under `--out` does not decode | `routeStarted`, `dayOf` | 2 |
| a notice's window closes fewer than `NoticeWorkingDays` (4) working days after it, with weekends and `non_working_days` left out, or has not yet closed (the check runs before the end of its close date in UTC) | `workingDays` | 2 |
| `responses` is absent (an empty list when none arrived), or a response was received before its notice | `Publication.Check` | 2 |
| a verbatim response has no `text` or `consent` file, or its text is not UTF-8 (8-bit Unicode Transformation Format); any other response has no `corrections` (an empty list when none were made) | `Publication.Check` | 2 |
| a correction is blank or runs over more than one line | `Publication.Check` | 2 |
| a route under `target.naming.permission_required` has no `permission` | `Publication.Check` | 2 |
| a route whose endpoint host ends in `.amazonaws.com` has no `replication_pack`. A replication pack has a blank recipient, does not hold `inventory.json` and every other file of the route's evidence pack with the same bytes, or is dated after its notice or before either UTC date a notice may not precede | `replicationRoute`, `evidence.PackCarries` | 2 |
| `disclosures` is absent (an empty list when none were made); a disclosure's `provider` is blank or runs over more than one line, its `recipient` is blank, its `how` is neither `email` nor `form`, or its date is not before the first notice | `Publication.Check` | 2 |
| `prerelease` is absent (an empty list when no copy was sent); a copy has blank `recipients` or no `copy` file, its `withheld` names a route outside the release, or it does not withhold every route that needs a replication pack | `Publication.Check` | 2 |
| a file the record names is missing, has a path outside the record's directory or a `sha256` that is not lowercase hex, or has bytes of another SHA-256 | `Publication.file` | 2 |
| a verbatim response's text, a correction or a disclosure's provider holds any notice's account or recipient, in any letter case, with white space and format characters (Unicode category Cf) ignored | `Publication.Check` | 2 |
| one of those texts holds the value of a confidential limit, the configured one or the one the refresh command read, in a form `config.ValueForms` gives (`config.Carries`) | `Publication.Check`, `withheldValues` | 2 |
| once the record passes, a copy of a ceiling-check record that `pages` made under `--out` is missing or differs; the first such copy is named | `copiedChecks` | 2 |
| the publication page does not build: a verbatim response's text no longer has its recorded SHA-256 | `Publication.Page` | 1 |
| with `--write-page`, `publication.md` or `publication.json` does not write; without it, one does not read for a cause other than its absence | `publishCheckCmd` | 1 |
| without `--write-page`, `publication.md` or `publication.json` is missing under `--pages` or is not the page the record gives | `publishCheckCmd`, `Publication.Page` | 2 |

With `--write-page` it writes `publication.md` and `publication.json` (`Publication.Page`). Neither holds a recipient or an account identifier.

| `publication.json` field (`PublicationPage`) | Content |
|---|---|
| `non_working_days` | the dates the record lists in `non_working_days`, which no notice window counted as working days; the notices paragraph of `publication.md` lists them, or prints `none` |
| `routes` | per route, `notice_sent`, `window_closes` (the date the window closed) and `named_with_permission`, and each response's date and corrections, with its text where it publishes verbatim |
| `corrections_log` | every correction, by route and the date of its response |
| `told_before` | the providers that learned of the measurement ahead of the notices, with how and when |
| `statements` | the fixed lines of `PublicationStatements` |

### What is published and what is retained

The last column of [the evidence files table](#hosted-runs-and-the-evidence-files) gives each file's status. The pages, the evidence pack, each `window-close.json` and these files a cell manifest names are published (SPEC-HOSTED §9), each cell's files as one compressed archive, `<route>-<cell>.tar.gz`, attached to a GitHub release of this repository and named on its route's page:

| Manifest field | File |
|---|---|
| `config` | the hash and size of `Config.Published`, the file `public-config` writes to its `--out`, under the name of the `--config` file the route ran with |
| `clock` | `clock.json` |
| `status_start`, `status_end` | `status-start.json`, `status-end.json` |
| `balance_start`, `balance_end` | `balance-start.json`, `balance-end.json` |
| `artifacts` | `attempts.jsonl`, `refresh.json`, `probe.json`, `dispatch.json`, `report.json`, `report.txt` |
| `release.probes` | the `ceiling-probe-<route>-<ceiling>-<ledger line>.json` copies |

The manifest also names files that stay retained. `frames` names the frame store. `operator` and `records` name the operator file and its records, whose copies `Operator.WriteCopies` writes beside the frames as `operator.json` and `records/`. Where the route has a confidential limit, these three carry `hmac_sha256` and no `bytes`. `ledger` gives the admission ledger's hash, size and line count; no release command writes the ledger into the published set.

The manifest names the source diff of a modified build by hash in `source.diff`, which publishes with the manifest, and `percentes --check-release` checks the diff's copy among the record copies beside the frames, which are retained (SPEC-HOSTED §9). `cell.json` is a working record the manifest does not name; its outcome reaches the published set through the manifest's `release` record.

### The end-to-end test

`TestReleaseLadderFiveMockRoutes` (`cmd/percentes-release/e2e_test.go`) runs five Shape S routes in one process against in-process shaped mocks over plain HTTP, one per form of `e2eForms`, with the ladder scaled to seconds and the host gates widened (`hostGatesWide`). It checks outcomes, records, settlement, manifests, the aggregate dispatch series, and per-step G2 and G6. Where the in-process client-validity gate is the only failure, it checks the attempted steps and skips, naming the gate parts that failed.

### The rehearsal

`percentes-release rehearse` serves shaped mocks on `127.0.0.1` over TLS under one `mock.SelfSignedCert`, the only root the client trusts (`Hooks.RootCAs`). The routes come from `rehearsalRoutes` and the forms from `rehearsalForms`; `--anthropic-rpm` sets the `anthropic` route's requests-per-minute (RPM) limit from `rehearsalRPMs`.

| Route | Form | Shape | Key header | Ceiling |
|---|---|---|---|---|
| `anthropic` | Messages, `max_tokens` | R | `x-api-key` | 16 |
| `azure` | chat completions, `max_completion_tokens`, reasoning | S | `api-key` | 512 |
| `azure-R` | the `azure` mock, same account group | R | `api-key` | 256, its fallback |
| `baseten` | chat completions, `max_tokens`, reasoning | S | bearer | 512 |
| `gemini` | chat completions, `max_tokens` | S | bearer | 512 |

1. Each route's ceiling check runs against its mock through `sendCeiling`, then the schedule freezes each route. Every route runs its first cell through `runCell` concurrently at its offset.
2. Attribution: one `azure-R` cell against `Server.SetRateLimit`, after `groupQuiet`; its `status_429` records must label at or below the limit's share (`labels`).
3. Overload: one `anthropic` step at 1.5 times its binding rate (`overload`, `route.overLimits`).
4. Failure mode: one `anthropic` ladder with the throttle and silent-hang faults armed (`failureMode`).
5. Check only: the `anthropic` and `gemini` ceiling checks against new mocks, with their ledger and records under `checks/` (`checkOnly`); `anthropic` is excluded and `gemini` takes 1,024.

The rehearsal is green when no step records a problem (`rehearse` sets `green` where its reasons are empty). `rehearsal.json` (`schema_version` 2) records the result with each reason.

`rehearse` refuses with exit 1, and writes nothing under `--out`, when:

- a flag does not parse, or `--out`, `--frames` or `--clock-sync` is missing (`rehearseCmd`);
- `--scale` is below 1, or makes a step shorter than G2's 5 s CPU window (`PinnedCPUWindowS`);
- `--anthropic-rpm` is not a value of `rehearsalRPMs` (5,000 or 10,000);
- `--byte-cap` is missing, or `--master-seed` is empty;
- `--frames` is inside the source tree, or `evidence.ParseClockCommand` refuses `--clock-sync`;
- `--out` holds white space, or exists and is not empty (`newDir`);
- the open-file soft limit is below `minOpenFiles` (`checkOpenFiles`);
- the binary carries no version-control revision, or was built from a modified tree and `--source-diff` is missing.

| Step | Each must hold | Function |
|---|---|---|
| before step 1 | each route's ceiling check is not blocked and does not exclude the route; the host-group and account-group checks pass (`checkHostGroups`, `checkAccountGroups`); `run`'s start checks pass for each route (`newRoute`); the ceiling rule admits every ladder step at the frozen binding rate. A failure here exits 1 with no `rehearsal.json` | `setUp`, `checkRoute`, `newRoute` |
| 1 to 4, every cell | a cell record with outcome `counted`; no ladder end (`client_validity`, `tripwire` or `backoff`); every ladder step its configuration sets attempted, with G2 (client validity) passing at each; a tripwire record with none fired. The completeness check passing (`evidence.CheckRelease`); `manifest.json` hashed; a replay of the report pair from `inputs/<route>.yaml` identical byte for byte | `rehearsed`, `replayCell` |
| 1 | each route's cell started at its offset with no execution error; its ceiling choice is the table's ceiling; its binding rate, binding limit and window are the release route's where `rehearsalRoutes` sets them (every route but `gemini`) | `full` |
| 1 | each mock answered status 200, and nothing else, to every ceiling-check and cell request its routes sent | `rehearse`, `maps1` |
| 1 | the host's aggregate dispatch series read from the cells' `dispatch.json` files | `aggregate` |
| 2 to 4 | no execution error; the cell's `attempts.jsonl` read | `stepCell` |
| 2 | a counted cell records at least one `status_429`; the mock answered 200 and 429 alone, their sum the requests sent and the 429 count the cell's `status_429` count | `attribution` |
| 2 | the route page labels every `status_429` of the cell, each at or below the limit's share; exactly one of the route's sends in its account group's history carries the first refusal's write time, and with that send's write time removed, the refusal is labelled `comparison unresolved` | `labels` |
| 3 | the mock answered 200 to every request; the resource bounds below held | `overload`, `rehearsalResources.problems` |
| 4 | both faults armed and recorded; where the cell attempted its top step, each fault fired and expired, the mock answered at least one 429 and one hung request, and the cell recorded a censored request; where it did not, the mock answered 200 to every request | `failureMode` |
| 4 | the mock answered 200, 429 and hung alone, their sum the requests sent and the 429 count the cell's `status_429` count; the resource bounds below held | `failureMode`, `rehearsalResources.problems` |
| 5 | `anthropic` excluded with a reason; `gemini` at Shape S's fallback ceiling; each new mock answered 200 to every check request it was sent | `checkOnly` |
| after step 5 | every route has a rehearsal cell to project; one window's projected disk below the free disk, the two stores' sum below it where `--out` and `--frames` share one filesystem and each store below its own otherwise; the free disk read | `rehearsalRun.windowDisk`, `windowDisk.problems` |

`windowDisk` projects one window from each route's largest rehearsal cell, taken `rehearsalAttemptsPerWindow` (14) times: its bytes under `--out` at `diskMargin` (2) times, and its frames at `diskMargin` times the route's largest ceiling over the events its mock streamed per request, or `diskMargin` where that ratio is below 1.

During steps 3 and 4 `hostSampler` (`cmd/percentes-release/rehearse_host.go`) reads, once a second, the process's open descriptors (`openFiles`) and the TCP sockets to each destination from `/proc/net/tcp` and `/proc/net/tcp6` (`socketsPerDestination`). Listening sockets are left out. Sockets in TIME_WAIT (the kernel's hold on a closed socket) are counted. The tables list every socket in the host's network namespace, so the count includes other processes' sockets.

At the cell's end `finish` reads the process's peak resident set size (RSS, `peakRSS`), the host's memory, the local port range (`localPortRange`) and the `tcp_tw_reuse` setting. `rehearsalResources.problems` holds each of the two cells to these bounds:

| Reading | Bound | A missing or failed reading |
|---|---|---|
| peak RSS | at most half the host's memory | breaks the bound on every host |
| open descriptors | below `minOpenFiles` (65,536) | breaks the bound on every host |
| sockets to one destination | below the size of the local port range | breaks the bound on Linux; leaves it unchecked on any other host |

A broken bound is a problem of its step's cell, and the rehearsal is not green. `--scale k` divides the ladder's durations, the offsets, the mocks' stream times and the ceiling checks' spacing by the whole number k, and the summary of a run with k above 1 carries `full_length` false.

### One release request, end to end

1. The schedule file gives the cell its date, slot and offset; `loadgen.BuildLadder` fixes the request's step and t_i; the attempt seed is the `<seed>` of its prefix `cs-<seed>-<index>`.
2. `runCell` admits the cell after its start checks; `gen.requestBody` puts the prefix and shape text in the frozen body's case slot.
3. `gen.execute` sends at t_i unless 50 ms late; the first byte takes a write permit; `liveSeries` counts the send.
4. The outcome (`in_time`, `late`, censored at 90 s, or an error class) is recorded in `attempts.jsonl` without the answer.
5. The cell report counts it on its step's line and towards Hosted-G6; `pages` pools the line into `routes/<route>.txt`, and the index prints the pooled count.

## Tracing a published number back to raw records

1. Find the line on `routes/<route>.txt` or in `routes/<route>.json`, or the row in `index.json`; each names its route, step and line.
2. Read the cells that contributed: the route's `<route>/<label>-a<attempt>/manifest.json` files whose `release` record has the outcome `counted`. The route's evidence pack holds these manifests under `out/` and its page files under `pages/`. The pack holds no `attempts.jsonl`, report pair or configuration, so steps 3 to 5 read the cell's archive, `<route>-<cell>.tar.gz`, which holds the files the cell's directory under `--out` holds, and the route's configuration.
3. Re-render one cell's report pair with `percentes --replay DIR --config FILE --out OUT`; it exits 1 unless the pair matches the manifest byte for byte. Where the route has a confidential limit, the configuration as published does not load, and only the retained `config.yaml` beside the cell's frames replays the cell.
4. Check the cell's files with `percentes --check-release DIR --config FILE --frames ROOT`, which confirms the recorded hash of each file [its list](#replay-and-the-completeness-check) names. It reads the retained frame store, so only the operator can run it; a route with a confidential limit also needs `--release-key`.
5. In that cell's `attempts.jsonl`, leave out records with `warmup` true and take the step's records. The cell has a figure for the step only where it attempted the step, that is where the step or a later one dispatched a request. Each record falls on one line (`lineOf`):
   - `outcome` empty (`""`): `unsent`;
   - `outcome` `censored`: `censored`;
   - `outcome` `errored`: `billing_refusal` where `billing_refusal` is true, else the line its `err_class` names;
   - `outcome` `completed`: `completed_in_time` where `in_time` is true, `completed_late` where `late` is true.

   The line's count over the step's records is the cell's figure.
6. For the route page's pooled figure, sum the step's line counts and its scheduled counts over the counted cells of the sampling frame that attempted the step with requests scheduled, or over one part where a drift split divides it (`pool`, `drift`).
7. For a record's timings, read its frames in the frame store (`frames.jsonl` under `--frames`, named in the manifest's `frames`), each read with its time from the run epoch; the frame store is retained, so this step is the operator's.
8. For a self-hosted run, find the field in the tables below; each row names the function that computes the field. A `*` in a field path stands for any key or index.

`report.json` (one run; `report.Report` embeds `run.Artifacts`):

| Field | Computed in |
|---|---|
| `windows.*`: `ttft_conditional_on_completion`, `e2e_conditional_on_completion`, `error_rate`, `goodput_frac`, `goodput_rps`, `goodput_sweep` | `collect.Collect` |
| `windows.*.completion_incidence` | `collect.EstimateIncidence` |
| `windows.*.ttft_tail_ci`, `e2e_tail_ci` (tail confidence intervals) | `collect.tailCIs`, called by `collect.Collect` |
| `threshold_analysis` | `collect.AnalyzeThresholds`, from the `baseline` and `fault` windows |
| `in_flight_at_fire` | `collect.AccountInFlight`, at `actual_fire_ns` |
| `share_gate` | `run.shareGate` |
| `detector` | `detect.RunWith` |
| `decomposition` | `detect.NewDecomposition`, with probe times recorded in `run.Execute` |
| `orchestration`, `actual_fire_ns` | `orchestrator.Execute`; `run.Execute` |
| `schedule_fired` | `run.scheduleFires`, which reads the mock's `/admin/faults` back |
| `loadgen.gates` | `loadgen.evaluateGates` |
| `validity_gates` | `validity.Evaluate` |
| `run_valid`, `invalid_reasons` | `run.validity`; `cmd/percentes` adds the reasons `validity.Report.FailReasons` gives for every gate except G1 and G2 |
| `conditional_headline` | `report.headline`; a hosted report has no such field |

`campaign.json` (`report.CampaignReport`, with the campaign under `campaign`):

| Field | Computed in |
|---|---|
| `campaign.per_run[*]` | `campaign.extractScalars`, one row per run |
| `campaign.endpoints[*]`, `campaign.invalid_runs` | `campaign.summarize`, each summary from `stats.Summarize` |
| `campaign.noise_floor_cov` (coefficient of variation of the primary endpoint, `clean_delete` campaigns only) | `campaign.RunWith`, from the `ttr_equilibrium_s` summary |
| `campaign.halted`, `halted_after_run`, `failed`, `failed_run`, `failed_reason` | `campaign.RunWith` |
| `instrument_commit`, `config_sha256`, `overrides` | `report.GenerateCampaignWith`, from the `report.CampaignMeta` that `cmd/percentes-campaign` passes |
| `validity_gates[*]` | `validity.Evaluate`, once per run in `cmd/percentes-campaign` |

`calibration.json` (`calibrate.Output`, with the result under `calibration`):

| Field | Computed in |
|---|---|
| `calibration.ramps[*].steps[*]`: `goodput`, `queue_mean`, `gates`, `server_window`, `receive_path` | `calibrate.LoadRunner.RunStep`, through `collect.Collect` and `collect.ReceivePathFor` |
| `calibration.ramps[*].steps[*]`: `pass`, `reasons` | `calibrate.Judge` |
| `calibration.ramps[*].lambda_max`, `valid`, `reason` | `calibrate.RunRamp` |
| `calibration.lambda_max`, `lambda_r`, `load_rate_rps`, `decision`, `valid`, `reason` | `calibrate.Calibrate` |
| `calibration.reference` | `calibrate.Reference` |

## Changing things safely

| Change | Where | Covered by |
|---|---|---|
| A configuration pin | one constant in `internal/config/config.go`, checked by `Config.Validate`; every file under `configs/` that sets the field | `TestPinnedValueEnforcement`; the reference configurations load in the Go suite; `make kind-smoke` and `make reproduce` for `configs/kind-e2e.yaml`; `make campaign-e2e` for `configs/kind-campaign.yaml`; `make process-kill-e2e` for `configs/process-kill-mock.yaml`; `percentes-calibrate --check` for `configs/phase1.yaml` |
| A metric | computed in `collect` or `detect`, carried in `run.Artifacts`, rendered in `report` | the §8 acceptance suite (`make test-ac`), whose AC6 asserts report completeness |
| A fault mode | `internal/mock/faults.go`, the configuration's fault enum, `/admin/faults` in `internal/mock/server.go`, `internal/mock/sse.go` | a behaviour test per mode in `internal/mock` |
| A release rule | `internal/admission`, `internal/probe`, `internal/evidence`, `internal/report`, `cmd/percentes-release` | `make test-unit`; `percentes-release rehearse` |
| A published field | the encoder and `report.FromRun` | `percentes --replay`; `evidence.CheckRelease` |

`make test` runs `test-unit`, `test-ac`, `kind-smoke`, `reproduce`, `campaign-e2e` and `process-kill-e2e`; continuous integration (`.github/workflows/ci.yml`) runs the same stages in its `unit`, `ac` and `kind` jobs and lint in `lint`, each job's `actions/setup-go` step pinning Go 1.21.6 (`go-version`), the toolchain the `Makefile` selects through `GOTOOLCHAIN`. `make hooks` sets `core.hooksPath` to `hooks`: `pre-commit` runs gofmt (the Go source formatter), build and lint, `commit-msg` checks the subject line, and `pre-push` runs the race unit suite.

## Glossary

| Term | Meaning | In the code |
|---|---|---|
| AC | an acceptance criterion of §8 | `internal/ac` |
| Account group | the routes of a release that share one provider account or deployment, and so its limits | `release.account_group` |
| Attempt | a cell's first scheduled run (attempt 1) or its re-run (attempt 2), each with its own date and seed | `schedule.Attempt`, `MaxAttempts` |
| Binding rate, binding limit | the highest rate every term of the binding-rate rule (part of the ceiling rule) admits, and the limit that sets it | `admission.Solve`; `admission.CeilingRule` tests each step at it; `binding_rps`, `binding_limit` |
| Byte cap | the length each Shape S ticket is cut at; a cap is spent once a ceiling check that ran at it reported a prompt above the input bound, and `run` refuses a `--byte-cap` above a spent cap | `probe.Spent` |
| Canary | a stream served at fixed times by a mock inside the client process, read beside the load by the load's own reader | `loadgen.StartCanary` |
| Case slot | the JSON string `"{{case}}"` a literal body carries once | `PilotCaseSlot` |
| Ceiling check | the ten-request check that chooses a host group's output ceiling | `probe.RunRelease`, `probe.ChooseCeilingAt` |
| Cell | one run of a route's ladder at one of the six slot hours on a weekday or a weekend date, plus the route's offset; a route has twelve | `runCell`, `evidence.Cell` |
| Confidential limit | a limit marked `confidential`, whose value is retained | `Limit.Marking` is `config.MarkingConfidential`; `Target.Confidential` reports a route with one |
| Deadline, hard timeout | release: 30 s from t_i splits in-time from late; 90 s from dispatch censors | `PinnedReleaseDeadlineS`, `PinnedReleaseHardTimeoutS` |
| Deciding record | among a host's records at one ceiling that were not stopped or superseded, the one that decides its check there: the only one, or the second where the first was refused | `probe.Decisive` |
| Drift split | a sampling frame divided at a change of tier, limits or model documentation | `report.DocsOf` |
| Expected model | `target.reported_model.name` where recorded, else `target.model_name` | `Target.ExpectedModel` |
| Fire anchor | whichever comes first of T_inject and the recorded fire | `collect.FireAnchorNs` |
| Frame store | the retained reads of each response body with their times, `frames.jsonl` under `--frames` | `evidence.WriteRunFrames` |
| Frozen line | the last ledger line the schedule read, by its count `ledger_lines` | `frozenEntries` |
| Goodput | the fraction of scheduled requests meeting the §4 objective | `collect.Collect` |
| Host group | the routes of a release serving one model with one shape | `release.other_hosts` |
| Ladder | a cell's warm-up and its five steps, each a measured window and a drain | `loadgen.BuildLadder` |
| lambda_max, lambda_r | measured single-replica capacity; the per-replica rate at 0.65 of it | `calibrate.Calibrate` |
| Quiet interval | the shortest wait after a refused check ends before it may be repeated | `probe.RepeatQuiet` |
| Release key | 32 random bytes keying every HMAC-SHA256 a release publishes | `internal/releasekey` |
| Route | one provider, model identifier, endpoint, account tier and request shape | `target.name` |
| Route stop | a cause that ends a route's process, held until acknowledged | `routeStop`, `route.holds` |
| Sampling frame | a route's twelve cells | `internal/schedule` |
| Shape R, Shape S | the release request shapes: a short fixed instruction, and a synthetic support ticket cut at the byte cap | `pilot.GenerateShapeR`, `pilot.GenerateShapeS` |
| Step | one ladder rate, a fraction of the binding rate | `PinnedReleaseStepFractions` |
| Superseded byte cap | a spent byte cap above the release's; a record at one decides nothing | `ceilingChoiceFor` |
| T_inject | the configured fault instant, the warm-up plus `fault.t_inject_offset_s` after the epoch | `TInjectNs` |
| t_i | request i's intended dispatch time | `Request.IntendedNs` |
| TTR | time to recovery, the held entry's start minus the fire anchor | `detect.RunWith` |
| Unsent | a request scheduled in an attempted step and never dispatched | `LineUnsent` |
| Victim | the replica the fault targets | `--victim`, `survivorOf` |
| Window-close record | a route's records read once more after its last cell | `evidence.NewWindowClose` |
