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
make hooks            # once per clone: continuous-integration (CI) gates on commit and push
make test-unit        # fast path: unit and integration tests only
make reproduce        # AC7: one-command full harness run against the local cluster
make test             # the whole gate: unit + AC suite + kind smoke + reproduce + campaign e2e
make percentes-pilot  # bin/percentes-pilot, the hosted pilot command
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
cmd/mockserver           fault-injectable mock inference server
cmd/naivesweep           standalone reconnaissance probe, outside the instrument
internal/                loadgen, collect, detect, orchestrator, validity, ...
                         hosted: probe, admission, evidence, pilot, score
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

[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#hosted-runs-and-the-evidence-files)
describes the probe, the ledger, each evidence file and the mock's shaped
form, and follows one request from its schedule to its line in the record
file.

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
