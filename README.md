# Percentes

*Pronounced per-SEN-teez. The measure, not the metric.*

Percentes measures what happens to a large language model (LLM)
inference service under sustained load and failure: the requests a
replica loss kills or strands, the degradation the surviving replica
takes on, and how long recovery takes, decomposed into measured
sub-phases (SPEC.md §1). The current fault class is replica loss in a
Kubernetes-served vLLM (an open-source LLM serving engine) deployment.
The same harness measures managed provider endpoints from one client
host under [SPEC-HOSTED.md](SPEC-HOSTED.md), the pre-registered hosted
protocol ([Measuring a hosted endpoint](#measuring-a-hosted-endpoint)).

The harness makes three commitments:

- **Open loop, coordinated-omission-correct.** Requests dispatch on a
  schedule fixed before the run; the generator never slows down because
  the system did, so a stalled system cannot hide its own stall. Every
  latency is re-based to the request's *intended* dispatch time.
- **Three-state outcomes.** Every scheduled request ends in exactly one
  of completed / errored / censored. Only completions enter latency
  histograms; failure rates are first-class; the completion curve is the
  Aalen-Johansen cumulative incidence over all scheduled requests, in
  which errors are competing terminal events and only timeouts are
  censored (SPEC §3).
  - A quantile the curve does not cross is refused, never extrapolated,
    in one of two forms decided by the window's ceiling (final completion
    incidence plus the event-free survival remaining where the curve ends):
    greater than the horizon where the ceiling reaches the quantile,
    unattainable where it does not.
- **Run-invalidating self-checks.** The client must show it was not
  the bottleneck (send-skew, undispatched-request, central processing
  unit (CPU), and garbage-collection (GC) pause gates, all pinned and
  run-failing). These are the client gates; the full run-validity set
  is SPEC §10. A run that cannot demonstrate its own validity exits
  invalid instead of publishing.

[SPEC.md](SPEC.md) is the authoritative, pre-registered specification:
every gate, tolerance, and detector parameter carries a pinned number,
enforced at config-load time: a configuration that weakens one refuses
to load. [CHANGELOG.md](CHANGELOG.md) lists every change to those rules
since first publication, dated. [SPEC-HOSTED.md](SPEC-HOSTED.md) is the
hosted protocol SPEC.md §6 requires before any provider data is
collected, with its amendment log. [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
maps each package to the spec clauses it serves and traces the report
fields and the evidence files to the code that computes them, with
diagrams.

## Scope

Phase 0 builds and certifies the measurement instrument against a mock
OpenAI-compatible server-sent events (SSE) server on a local kind
cluster: passing the acceptance-criteria (AC) suite certifies the
measurement machinery only; real graphics processing unit (GPU) claims
wait for Phase 1, which characterizes real vLLM on GPU hardware with the
same harness and gates. Phase 1's §10 calibration procedure ran on one
L40 on 16 September 2026 against a standalone container; the calibration
inside the experiment's Kubernetes environment and the characterization
runs have not.

## Quickstart (no GPU required)

Requires Go 1.21 or newer, Docker, [kind](https://kind.sigs.k8s.io/),
kubectl, python3 and curl. The Makefile and the git hooks select Go
1.21.6, which is the toolchain the pinned linter can read; set
`GOTOOLCHAIN` yourself to override.

```
make hooks                    # once per clone: continuous-integration (CI) gates on commit and push
make test-unit                # fast path: unit and integration tests only
make reproduce                # AC7: one-command full harness run against the local cluster
make test                     # the whole gate: unit + AC suite + kind smoke + reproduce + campaign e2e
make percentes-pilot          # bin/percentes-pilot, the hosted pilot command
make percentes-release        # bin/percentes-release, the release runner for this host
make percentes-release-linux  # a static linux/amd64 build of it for the client host
```

In that order: `make hooks` before your first commit, `make test-unit` to see
the tree is sound, then the cluster targets. `make test` runs all of them.

`make hooks` points git at `hooks/`: gofmt, build and golangci-lint on
commit (`go vet` when the linter is absent), and the race unit suite on
push, so a change fails locally before it fails CI. `hooks/commit-msg`
requires a subject line of 55 characters or fewer in lowercase
conventional style, with no body.

The full gate's last three stages build a Docker image and drive a kind
cluster. The AC suite measures this machine as well
as the code: before its timing-coupled tests it qualifies the host with
a probe (timer wake lateness and garbage-collection pauses,
`internal/hostqual`). On a host that fails the probe those tests skip and
`make test-ac` fails; `PERCENTES_AC_ALLOW_SKIPS=1` accepts the skips. When
a timing-coupled assertion fails, the host is probed again: a host that
lost qualification during the run skips the test, and one that kept it
fails.
The cluster stages bind host ports 18080 to 18082, which `SVC_PORT`,
`POD_PORT` and `ADMIN_PORT` override, and host port 18000, the kind
mapping to the mock Service's NodePort 30800, which is fixed in
`deploy/kind/kind-config.yaml`. On macOS the host CPU gate reports
unmeasured, so every cluster run prints `RUN INVALID (run-failing gate)`
and exits 2 while the stage itself passes: the gates are enforced, and
this platform cannot supply one of their inputs. Reports are written to
`results/`.

## Layout

```
cmd/percentes            single-run harness command-line interface (CLI); hosted runs, replay and the completeness check
cmd/percentes-campaign   N-run campaign runner (SPEC §5 repetition, §10 gates)
cmd/percentes-calibrate  SPEC §10 capacity ramp and §5 single-replica reference
cmd/percentes-pilot      the SPEC-HOSTED HA2 pilot: ledger, probe, dev, session, check-cpu
cmd/percentes-release    one route of a SPEC-HOSTED HA3 release: run, dispatch, check-cpu
cmd/mockserver           fault-injectable mock inference server
cmd/naivesweep           standalone reconnaissance probe, outside the instrument
internal/                loadgen, collect, detect, orchestrator, validity, ...
                         hosted: probe, admission, evidence, pilot, score, schedule
configs/                 pinned configurations; a *.reference.yaml per profile
deploy/                  kind, mock and Phase 1 manifests; reproduce and capture scripts
docs/                    ARCHITECTURE.md and the draw.io diagrams under docs/diagrams/
```

## Measuring a hosted endpoint

A hosted target (`target.hosted: true`) is a provider's managed endpoint.
SPEC-HOSTED.md pins what a hosted run sends and records; the `pilot`
configuration profile carries the pins of its amendment HA2 (hosted
amendment 2) and refuses a value outside them. Every hosted run writes,
beside the report pair, the evidence SPEC-HOSTED §9 requires: a
per-request record file (`attempts.jsonl`), the raw response frames in a
store outside the source tree, the client clock offset and the endpoint
status at start and end, the pre-run probe's record, and a manifest that
hashes them all with the configuration, the operator's documentation
records and the admission ledger's position. The report pair is rendered
from those files alone, so `percentes --replay DIR --config FILE` renders
it again and fails unless the result matches byte for byte, and
`percentes --check-release DIR --config FILE --frames ROOT` refuses a run
whose files are missing or differ from their recorded hashes.

`percentes-pilot` runs the descriptive pilot of SPEC-HOSTED amendment HA2:
two endpoints, one task, six paired sessions on dates derived from a
master seed. Keys come from a file given with `--env` (`NAME=value`,
readable by its owner only) and are never printed or recorded; every
request is reserved in the admission ledger before it is sent. The
command's package documentation lists its subcommands and exit codes:

```
go doc ./cmd/percentes-pilot
```

`percentes-release` runs one route of a public descriptive release under
SPEC-HOSTED amendment HA3 (hosted amendment 3). A route is one provider,
one model, one endpoint and one account tier; its configuration carries
the complete request uniform resource locator (URL), the whole request
body as literal JavaScript Object Notation (JSON) frozen by hash, the
header the key travels in, and every limit recorded for the account with
its source. Twelve cells per route run over a committed window, one per
Coordinated Universal Time (UTC) slot on a weekday and on a weekend day,
each cell a ladder of five offered-load steps (Poisson arrivals, random
and independent, at 0.25, 0.5, 1, 2 and 4 requests per second, 480 s
each, with a 90 s drain after every step). A step is admitted only where
its nominal demand is at most half of every recorded rate, token,
concurrency, daily and spend-window limit, and only where the account's
monthly cap and the route's liability cap cover its twelve cells,
re-runs and probes. Every finished cell writes a labelled manifest and
report pair; a cell that does not count is kept as a record outside the
route's frame. The routes of a release run as separate processes on one
client host, each at its own offset inside the slot.

**One request, end to end.** The committed schedule gives the route a
cell (a date and a UTC slot) and its offset inside the slot, and the
cell's seed fixes the request's step and intended time. Its user text is
the prefix `cs-<seed>-<index>` (six-digit index), a space and a synthetic
support ticket; the frozen body carries it in its one slot. The client
sends at the intended time, and the 90 s hard timeout runs from the send.
The stream's first content event stamps the time to first token;
`data: [DONE]` ends a chat-completions stream and `message_stop` a
Messages stream; a whole answer arriving more than 30 s after the
intended time is late. The request is one line of `attempts.jsonl` with
its step, `in_time` or `late`, status, finish reason and usage, and one
count on its step's `completed_in_time` or `completed_late` line in the
cell's `report.json`, over the step's scheduled requests with a 95
percent binomial (Wilson score) interval. A route's cells pool that line
per step, and the index table, alphabetical with counts only, shows each
step's pooled count and its denominator.

[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#hosted-runs-and-the-evidence-files)
describes the probe, the ledger, each evidence file and the mock's shaped
form, and [its release section](docs/ARCHITECTURE.md#the-release-runner)
the profile's pins, the schedule, the ceiling rule and check, a cell's
outcomes and files, the per-step tables and the index.

## Pointing the probe at an endpoint

`cmd/naivesweep` sweeps one OpenAI-compatible endpoint and reports what came
back, including responses that returned 200 and carried nothing. It is
reconnaissance, and nothing it prints is a published measurement. Its
configuration, its limits and what it redacts are in its own documentation:

```
go doc ./cmd/naivesweep
```

## License

Apache-2.0. If you build on the harness or the methodology, cite the
exact commit and configuration of the run you reproduce.
