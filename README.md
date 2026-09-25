# Percentes

*Pronounced per-SEN-teez. The measure, not the metric.*

Percentes measures what happens to a hosted LLM inference service under
sustained load and failure: the requests a replica loss kills or
strands, the degradation the surviving replica takes on, and how long
recovery takes, decomposed into measured sub-phases (SPEC.md §1). The
current fault class is replica loss in a Kubernetes-served vLLM
deployment.

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
- **Run-invalidating self-checks.** The client must prove it was not
  the bottleneck (send-skew, undispatched-request, CPU, and GC-pause
  gates, all pinned and run-failing). These are the client gates; the
  full run-validity set is SPEC §10. A run that cannot demonstrate its
  own validity exits invalid instead of publishing.

[SPEC.md](SPEC.md) is the authoritative, pre-registered specification:
every gate, tolerance, and detector parameter carries a pinned number,
enforced at config-load time: a configuration that weakens one refuses
to load. [CHANGELOG.md](CHANGELOG.md) lists every change to those rules
since first publication, dated. [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
maps each package to the spec clause it serves and shows how to trace any
reported number to its source in the code.

## Scope

Phase 0 builds and certifies the measurement instrument against a mock
OpenAI-compatible SSE server on a local kind cluster: passing the
acceptance suite certifies the measurement machinery only;
real-GPU claims wait for Phase 1, which characterizes real vLLM on GPU
hardware with the same harness and gates. Phase 1's §10 calibration procedure
ran on one L40 on 16 September 2026 against a standalone container; the
calibration inside the experiment's Kubernetes environment and the
characterization runs have not.

## Quickstart (no GPU required)

Requires Go 1.21 or newer, Docker, [kind](https://kind.sigs.k8s.io/),
kubectl, python3 and curl. The Makefile and the git hooks select Go
1.21.6, which is the toolchain the pinned linter can read; set
`GOTOOLCHAIN` yourself to override.

```
make hooks       # once per clone: run the CI fast gates on commit and push
make test-unit   # fast path, a few minutes: unit/integration tests only
make reproduce   # AC7: one-command full harness run against the local cluster
make test        # the whole gate: unit + AC suite + kind smoke + reproduce + campaign e2e
```

In that order: `make hooks` before your first commit, `make test-unit` to see
the tree is sound, then the cluster targets. `make test` runs all of them and
takes tens of minutes.

`make hooks` points git at `hooks/`: gofmt, build and golangci-lint on
commit, and the race unit suite on push, so a change fails locally before
it fails CI. `hooks/commit-msg` requires a subject line of 55 characters
or fewer in lowercase conventional style, with no body.

The full gate takes tens of minutes and its last three stages build a
Docker image and drive a kind cluster. `make test-unit` is the fast path.
The AC suite measures this machine as well as the code, so on a busy host
the tests that depend on the §2 client-validity gate report SKIP rather
than a defect; a green run with skips has not certified those criteria.
The cluster stages bind host ports 18080 to 18082, which `SVC_PORT`,
`POD_PORT` and `ADMIN_PORT` override. On macOS the host CPU gate reports
unmeasured, so every cluster run prints `RUN INVALID (run-failing gate)`
and exits 2 while the stage itself passes: the gates are enforced, and
this platform cannot supply one of their inputs. Reports are written to
`results/`.

## Layout

```
cmd/percentes            single-run harness CLI
cmd/percentes-campaign   N-run campaign runner (SPEC §5 repetition, §10 gates)
cmd/percentes-calibrate  SPEC §10 capacity ramp and single-replica reference
cmd/mockserver           fault-injectable mock inference server
cmd/naivesweep           standalone reconnaissance probe, outside the instrument
internal/                loadgen, collect, detect, orchestrator, validity, ...
configs/                 pinned reference configurations
deploy/                  kind + mock manifests, reproduce scripts, Phase 1 vLLM manifest
```

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
