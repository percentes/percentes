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
  censored (SPEC §3). A quantile the curve does not cross is refused,
  never extrapolated, in one of two forms decided by the window's ceiling
  (final completion incidence plus the censored mass still outstanding at
  the horizon): beyond the horizon where the ceiling reaches the quantile,
  unattainable where it does not.
- **Run-invalidating self-checks.** The client must prove it was not
  the bottleneck (send-skew, CPU, and GC-pause gates, all pinned and
  run-failing). A run that cannot demonstrate its own validity exits
  invalid instead of publishing.

[SPEC.md](SPEC.md) is the authoritative, pre-registered specification:
every gate, tolerance, and detector parameter carries a pinned number,
enforced at config-load time: a configuration that weakens one refuses
to load. [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) maps each package
to the spec clause it serves and shows how to trace any reported number
to its source in the code.

## Scope

Phase 0 (complete) builds and certifies the measurement instrument
against a mock OpenAI-compatible SSE server on a local kind cluster:
passing the acceptance suite certifies the measurement machinery only;
real-GPU claims wait for Phase 1, which characterizes real vLLM on GPU
hardware with the same harness and gates.

## Quickstart (no GPU required)

Requires Go 1.21+, Docker, and [kind](https://kind.sigs.k8s.io/).

```
make test        # the whole gate: unit + AC suite + kind smoke + reproduce + campaign e2e
make test-unit   # fast path: unit/integration tests only
make reproduce   # AC7: one-command full harness run against the local cluster
make hooks       # once per clone: run the CI fast gates on commit and push
```

`make hooks` points git at `hooks/`: gofmt, build and golangci-lint on
commit, and the race unit suite on push, so a change fails locally before
it fails CI.

## Layout

```
cmd/percentes            single-run harness CLI
cmd/percentes-campaign   N-run campaign runner (SPEC §5 repetition, §10 gates)
cmd/mockserver           fault-injectable mock inference server
internal/                loadgen, collect, detect, orchestrator, validity, ...
configs/                 pinned reference configurations
deploy/                  kind + mock manifests, reproduce scripts, Phase 1 vLLM manifest
```

## License

Apache-2.0. If you build on the harness or the methodology, cite the
exact commit and configuration of the run you reproduce.
