# Percentes

*Pronounced per-SEN-teez. The measure, not the metric.*

Percentes is a measurement instrument for large language model (LLM)
inference services. It sends requests on a schedule fixed in advance,
records what happened to every one of them, and reports the result with
the evidence a reader needs to recompute it. It serves two measurements:

- **A self-hosted service under failure.** [SPEC.md](SPEC.md) §1 asks
  what a replica loss costs a Kubernetes-served vLLM (an open-source LLM
  serving engine) deployment: the requests the loss kills or strands, the
  degradation the surviving replica takes on, and how long recovery
  takes, split into measured sub-phases.
- **A provider's hosted application programming interface (API) under
  load.** [SPEC-HOSTED.md](SPEC-HOSTED.md) measures a managed endpoint
  from one client host through one ordinary paid account: how many
  scheduled requests complete in time, how many fail and how, at five
  offered-load steps, each sending its requests at equal intervals and
  each a fraction up to 1.0 of the route's binding rate: the highest
  rate at which the nominal demand (the requests, and their tokens and
  spend at the worst case per request) stays within every rate, token,
  concurrency and rolling spend limit in each evaluation window the
  provider documents, and every daily limit over each of the provider's
  reset days, and every cap recorded for the account
  ([Measuring a hosted endpoint](#measuring-a-hosted-endpoint)).

You need the first if you run an inference service and want to know
what one lost replica does to live traffic. You need the second if you
buy LLM API capacity and set retries, fallbacks and timeouts from how
the provider fails.

Three rules shape every measurement:

- **Open loop.** Every request's send time is fixed before the run, and
  the generator never waits for an answer before the next send. If it
  waited for slow answers, it would send fewer requests exactly while
  the service is slow and under-count that period, an error known as
  coordinated omission. Every latency is measured from the request's
  *intended* send time.
- **Three-state outcomes.** Every dispatched request ends in exactly one
  of completed, errored or censored (observed to the client timeout with
  no terminal event: the pinned 30 s, or the hosted release's 90 s hard
  timeout). A scheduled request left unsent, by a stop or, under the
  release profile, for being more than 50 ms late, is reported on its
  own line. Only completions enter latency histograms. Failure rates are
  reported as figures in their own right.

  The completion curve gives, for each time t after the intended send,
  the share of all scheduled requests completed by t, estimated with the
  Aalen-Johansen cumulative incidence estimator. Errors are competing
  terminal events on the curve. A timeout is censored at the moment it
  timed out, and under the release profile a scheduled request left
  unsent is censored at the curve's horizon, the end of the observed
  time (SPEC §3, SPEC-HOSTED §5).
  - A latency quantile the completion curve never reaches inside the
    horizon is refused and is never extrapolated. Add the share of
    scheduled requests completed when the curve ends to the share still
    without an ending (censored). Where that sum reaches the quantile,
    the report prints the quantile as greater than the horizon. Where it
    does not, the errored requests alone hold the curve below the
    quantile, and the report prints it as unattainable.
- **Run-invalidating self-checks.** The client must show it was not the
  bottleneck: send-skew, undispatched-request, central processing unit
  (CPU) and garbage-collection (GC) pause gates, all pinned and
  run-failing. These are the client gates; the full run-validity set is
  SPEC §10. A self-hosted run that fails these checks exits invalid. A
  release cell that fails them retains its evidence with a
  client-invalid label and does not count toward frame completeness
  (the twelve counted cells a route needs before it pools).

| Document | What it is | Read it when |
|---|---|---|
| [SPEC.md](SPEC.md) | the authoritative, pre-registered specification of the replica-loss experiment: every gate, tolerance and detector parameter carries a pinned number, enforced when a configuration loads, so a configuration that weakens one refuses to load | you want to know what a self-hosted run measures, or why a configuration refused to load |
| [CHANGELOG.md](CHANGELOG.md) | every change to those rules since first publication, dated | you want to know what changed and when |
| [SPEC-HOSTED.md](SPEC-HOSTED.md) | the hosted protocol SPEC.md §6 requires before any provider data is collected; its body carries the adopted amendments, its amendment log and Appendix A record them, and it opens with a plain summary for a first-time reader | you want to know what a hosted figure means |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | how the code implements both documents: the packages, the commands, the evidence files, and one request followed from its schedule to its line in the report, with diagrams | you want to change the code or check a published number against it |

## Scope

Percentes is built in phases. Phase 0 builds and certifies the
measurement instrument against a mock OpenAI-compatible server-sent
events (SSE) server on a local kind cluster. Passing the
acceptance-criteria (AC) suite certifies the measurement machinery only;
real graphics processing unit (GPU) claims wait for Phase 1, which
characterizes real vLLM on GPU hardware with the same harness and gates.
Phase 1's SPEC §10 calibration procedure ran on 16 September 2026 on one
NVIDIA L40 GPU against a standalone container; the calibration inside
the experiment's Kubernetes environment and the characterization runs
have not yet run.

## Quickstart (no GPU required)

This section gets the tree built and tested on one machine. Requires Go
1.21 or newer, Docker, [kind](https://kind.sigs.k8s.io/), kubectl,
python3 and curl. The Makefile and the git hooks select Go 1.21.6, which
is the toolchain the pinned linter can read; set `GOTOOLCHAIN` yourself
to override.

```
make hooks                    # once per clone: continuous-integration (CI) gates on commit and push
make test-unit                # fast path: unit and integration tests only
make reproduce                # AC7: one-command full harness run against the local cluster
make test                     # the whole gate: unit + AC suite + kind smoke + reproduce + campaign end-to-end (e2e) + process-kill e2e
make percentes-pilot          # bin/percentes-pilot, the hosted pilot command
make percentes-release        # bin/percentes-release, the release runner for this host
make percentes-release-linux  # a static linux/amd64 build of it for the client host
```

In that order: `make hooks` before your first commit, `make test-unit` to see
the tree is sound, then the cluster targets. `make test` includes `make test-unit` and `make reproduce`.
The two hosted binaries are needed only for the hosted measurement.

`make hooks` points git at `hooks/`: gofmt, build and golangci-lint on
commit (`go vet` when the linter is absent), and the race unit suite on
push, with the 30-minute test timeout CI gives it, so a change fails
locally before it fails CI. `hooks/commit-msg` requires a subject line
of 55 characters or fewer in lowercase conventional style, with no
body.

The full gate's last four stages build a Docker image. Three of them
drive a kind cluster; the process-kill stage runs the mock in a local
Docker container on host port 18100, which `PK_PORT` overrides, and
kills the container's init process. The AC suite measures this machine
as well as the code: before its timing-coupled tests it qualifies the
host with a probe (timer wake lateness and garbage-collection pauses,
`internal/hostqual`).

On a host that fails the probe those tests skip and `make test-ac`
fails; `PERCENTES_AC_ALLOW_SKIPS=1` accepts the skips. When a
timing-coupled assertion fails, the host is probed again: a host that
lost qualification during the run skips the test, and one that kept it
fails.

The cluster stages bind host ports 18080 to 18082, which `SVC_PORT`,
`POD_PORT` and `ADMIN_PORT` override, and host port 18000, the kind
mapping to the mock Service's NodePort 30800, which is fixed in
`deploy/kind/kind-config.yaml`. On macOS the stages that run the harness
build it with `CGO_ENABLED=0`, which disables cgo (the Go mechanism for
calling C code), and a macOS build without cgo reads no host CPU
samples, so the host CPU gate reports unmeasured.

Every run in those stages is then invalid, and the command that ran it
exits 2 while the stage itself passes; `percentes` prints
`RUN INVALID (run-failing gate)` and `percentes-campaign` prints
`CAMPAIGN NOTE: at least one run is invalid`. Reports are written to
`results/`.

## Layout

```
cmd/percentes            single-run harness command-line interface (CLI) for a self-hosted target; replay and the completeness check of a release cell
cmd/percentes-campaign   N-run campaign runner for a self-hosted target (SPEC §5 repetition, §10 gates)
cmd/percentes-calibrate  SPEC §10 capacity ramp and §5 single-replica reference
cmd/percentes-pilot      the SPEC-HOSTED hosted amendment 2 (HA2) pilot: ledger, probe, dev, session, check-cpu
cmd/percentes-release    a SPEC-HOSTED HA3 and HA4 (hosted amendments 3 and 4) release: key, schedule, ledger, ceiling-check, public-config, run, dispatch, pages, pack, publish-check, rehearse, check-cpu
cmd/mockserver           fault-injectable mock inference server
cmd/naivesweep           standalone reconnaissance probe, outside the instrument
internal/                loadgen, collect, detect, orchestrator, validity, ...
                         hosted: probe, admission, evidence, pilot, score, schedule, releasekey
configs/                 pinned configurations; a *.reference.yaml per profile
deploy/                  kind, mock and Phase 1 manifests; reproduce and capture scripts
deploy/release/          added by a release's freeze commit: pct-refresh.py, which reads the provider pages, and the refresh-<route> wrappers that run it, one per provider (azure-R runs refresh-azure)
docs/                    ARCHITECTURE.md and the draw.io diagrams under docs/diagrams/
```

<a id="measuring-a-hosted-endpoint"></a>

## The hosted measurement

Under [SPEC-HOSTED.md](SPEC-HOSTED.md), a hosted release measures each
route (one provider, model, endpoint, account tier and request shape) in
twelve cells inside a window of at most 14 days: six UTC start slots,
each run once on a weekday and once on a weekend day. A cell is one run
of five load steps at 0.25, 0.5, 0.75, 0.9 and 1.0 of the route's
binding rate, with no retries. A 60 s warm-up comes first, and each step
runs for 240 s and is followed by a 90 s drain in which nothing is sent:
28 minutes 30 seconds when all five steps run. A completion within 30 s
of the intended send is in time and a later one is late; a request with
no terminal event 90 s after its actual send is censored. On a route's
published page, each outcome line of a cell gives the outcome's name,
its count over the step's scheduled requests, the percentage and a
95 percent Wilson interval, the range of rates consistent with the
count. The page pools counts across cells, step by step, only for a
route with twelve counted cells.

**Checking a published figure.** Each cell's published files are one
archive, `<route>-<cell>.tar.gz`, on a GitHub release of this
repository. `make bins` builds `bin/percentes`. With a cell's files,
`bin/percentes --replay DIR --config FILE --out OUT` renders the cell's
report again and exits 1 unless both report files match the hashes the
cell's manifest records. DIR is the cell's directory, FILE the route's
configuration as published and OUT a directory other than DIR. A route
with a confidential limit (a value only the account console shows, under
terms that keep it confidential) publishes its configuration with
`"[retained]"` in place of the value; that file does not load, so only
the operator can replay its cells.
[Tracing a published number back to raw records](docs/ARCHITECTURE.md#tracing-a-published-number-back-to-raw-records)
gives the steps from a line on a route page to the records behind it,
and names the checks that need unpublished files.

**Running a release.** [docs/HOSTED-RELEASE.md](docs/HOSTED-RELEASE.md)
lists the release commands and when each runs, with the operator rules
for keys, confidential limits and the admission ledger.
The operating rule for a confidential limit: never write its value, in
any format, into a configuration comment, the operator file (the file
that names a run's provider, account tier, client host and record
files), a note, a URL or a file name.

## Sweeping an endpoint outside the instrument

`cmd/naivesweep` sweeps one OpenAI-compatible endpoint and reports what came
back, including responses that returned 200 and carried nothing. It is
reconnaissance, and nothing it prints is a published measurement. It
admits nothing to a release's ledger, so a sweep on a paid account
spends outside every release cap; keep it off release accounts during a
collection window. Its configuration, its limits and what it redacts are
in its own documentation:

```
go doc ./cmd/naivesweep
```

## License

Apache-2.0. If you build on the harness or the methodology, cite the
exact commit and configuration of the run you reproduce.
