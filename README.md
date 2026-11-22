# Percentes

*Pronounced per-SEN-teez. The measure, not the metric.*

Percentes is a measurement instrument for large language model (LLM)
inference services. It sends requests on a schedule fixed in advance,
records what happened to every one of them, and reports the result with
the evidence a reader needs to recompute it. It serves two measurements:

- **A self-hosted service losing a replica.** This measurement is for
  an engineer who runs vLLM (an open-source LLM serving engine) on
  Kubernetes and wants to know what one lost replica does to live
  traffic. It reports the requests the loss kills or strands, the
  degradation the surviving replica takes on, and how long recovery
  takes, split into measured sub-phases. The rules are in
  [SPEC.md](SPEC.md).
- **A provider's hosted API under load.** This measurement is for an
  engineer who buys LLM API capacity and sets retries, fallbacks and
  timeouts from how the provider fails. Percentes sends five steps of
  load from one client host through one ordinary paid account. At each
  step it counts the scheduled requests that completed in time,
  completed late, errored (by error class), timed out at the client or
  were left unsent. The top step runs at the binding rate, which
  is fixed in advance from the limits and caps recorded for the account,
  with each request's tokens and cost taken at the worst case. The rules
  are in [SPEC-HOSTED.md](SPEC-HOSTED.md), which opens with
  [a plain summary](SPEC-HOSTED.md#read-this-first).

## Status

**Self-hosted.** Two results are published, each from a single vLLM
replica on one NVIDIA L40 GPU with no Kubernetes: a
[calibration](https://percentes.ai/writing/2026/calibrating-one-l40-configuration/)
on 16 September 2026 and a
[process-kill campaign](https://percentes.ai/writing/2026/i-sigkilled-vllm-on-an-l40-under-load-it-served-again-after-42-seconds/)
of five runs on 2 October 2026. The replica-loss experiment, with two
replicas on Kubernetes, has not yet run.

**Hosted.** No hosted result has been published.

**Self-hosted phases.**
[Not implemented](docs/ARCHITECTURE.md#not-implemented) lists what the
code does not yet do against real vLLM.

- **Phase 0, against a mock.** The instrument is built and certified
  against a mock OpenAI-compatible server-sent events (SSE) server. The
  acceptance-criteria (AC) suite of SPEC.md §8 passed in
  [CI on 16 November 2026](https://github.com/percentes/percentes/actions/runs/43935064489):
  AC1 to AC6 against the mock as a local process, and AC7 against two
  mock replicas on a kind cluster. A pass certifies the measurement
  machinery only.
- **Phase 1, real vLLM on GPU hardware.** Phase 1 uses the same harness
  and gates as Phase 0. The calibration ran the SPEC.md §10 procedure
  against a standalone container. In the process-kill variant
  (SPEC.md §1) a single replica's server process is killed and its
  container restarts in place; all five runs were valid. The calibration
  inside the experiment's Kubernetes environment and the two-replica
  characterization runs are pending.

**Hosted amendments.** Hosted amendments 1 and 3 to
[SPEC-HOSTED.md](SPEC-HOSTED.md) were adopted on 19 October 2026 and
hosted amendment 4 on 16 November 2026, each before any provider data
was collected under that document. Hosted amendment 2 is a draft and is
not adopted.

## How it measures

- **Open loop.** Every request's send time is fixed before the run, and
  the generator never waits for an answer before the next send. If it
  waited for slow answers, it would send fewer requests exactly while
  the service is slow and under-count that period, an error known as
  coordinated omission. Time to first token and time to completion are
  measured from the request's *intended* send time.
- **Three-state outcomes.** Every dispatched request ends in exactly one
  of completed, errored or censored (observed to the client timeout with
  no terminal event). Only completions enter latency histograms, and
  failure rates are reported as separate figures. The completion curve
  gives, for each time after the intended send, the share of all
  scheduled requests completed by then. A latency quantile the curve
  never reaches is refused and is never extrapolated ([SPEC.md](SPEC.md)
  §3).
- **Run-invalidating self-checks.** The client must show it was not the
  bottleneck: pinned gates on send lateness, unsent requests, CPU use
  and garbage-collection pauses ([SPEC.md](SPEC.md) §2; §10 lists the
  full run-validity set). A self-hosted run that fails one exits
  invalid. A hosted cell (one run of the five load steps) that fails one
  keeps its evidence and does not count toward the twelve cells
  described under [The hosted measurement](#the-hosted-measurement).

## Documents

| Document | Read it when |
|---|---|
| [SPEC.md](SPEC.md) | you want the pre-registered rules of the replica-loss experiment, or to know why a configuration refused to load: every gate, tolerance and detector parameter is a pinned number, and a configuration that weakens one does not load |
| [SPEC-HOSTED.md](SPEC-HOSTED.md) | you want to know what a hosted figure means; start at [Read this first](SPEC-HOSTED.md#read-this-first) |
| [CHANGELOG.md](CHANGELOG.md) | you want every change to SPEC.md's rules since first publication, dated |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | you want to change the code or check a published number against it |
| [docs/HOSTED-RELEASE.md](docs/HOSTED-RELEASE.md) | you run a hosted release |
| [deploy/phase1/README.md](deploy/phase1/README.md) | you deploy vLLM on GPU hardware for a Phase 1 run |
| [SPEC-HOSTED-AMENDMENT-DRAFT.md](SPEC-HOSTED-AMENDMENT-DRAFT.md) | you want hosted amendment 2, which specifies a privately reported pilot and is drafted and not adopted |

## Quickstart (no GPU required)

Requires Go 1.21 or newer, Docker, [kind](https://kind.sigs.k8s.io/),
kubectl, python3 and curl.

`make reproduce` and `make test` act on the kind cluster named
`percentes`; set `CLUSTER` to use another name. Both create the cluster
if none of that name exists. If one exists and its control-plane node
lacks the mapping from container port 30800 to host port 18000,
`make reproduce` deletes that cluster and creates it again, whether run
alone or as a stage of `make test`. Each stage that deploys to the
cluster first deletes the cluster's `percentes` namespace, and each
stage that runs the harness first deletes its previous output under
`results/`.

```
make test-unit    # fast path: unit and integration tests only
make reproduce    # one full harness run against mock replicas on kind
make test         # the whole gate: every stage, including the two above
```

`make reproduce` deploys two mock replicas on the cluster and runs the
harness against them once, with a fault injected on one replica. The
run's report is `results/kind-e2e/report.txt`, with `report.json` beside
it, and the stage checks the pair.

The `reproduce`, `campaign-e2e` and `process-kill-e2e` stages write
their reports under `results/` and build the command they run without
cgo (the Go mechanism for calling C code). On macOS such a build reads
no host CPU samples, so the host CPU gate reports unmeasured and every
run in those stages is invalid. The command exits 2 and the stage itself
passes. In `reproduce`, `percentes` prints a line starting
`RUN INVALID (run-failing gate):`.

[Running the test stages](docs/ARCHITECTURE.md#running-the-test-stages)
covers the git hooks, the Go toolchain the Makefile selects, the host
ports each stage binds, the host probe the AC suite runs first and the
line each command prints on macOS.

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

## Code layout

The seven commands under `cmd/` and the packages under `internal/` are
listed in docs/ARCHITECTURE.md:
[Binaries](docs/ARCHITECTURE.md#binaries) and
[Package map](docs/ARCHITECTURE.md#package-map). One command,
`cmd/naivesweep`, sweeps one OpenAI-compatible endpoint and reports what
came back. It is reconnaissance outside the instrument, and nothing it
prints is a published measurement; `go doc ./cmd/naivesweep` gives its
configuration, its limits and what it redacts.

## License

Apache-2.0. If you build on the harness or the methodology, cite the
exact commit and configuration of the run you reproduce.
