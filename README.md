# Percentes

*Pronounced per-SEN-teez. The measure, not the metric.*

Percentes measures what happens to a hosted LLM inference service under
sustained load and failure: how many in-flight and queued requests fail
or time out when a serving replica is lost, how the survivors degrade,
and how long recovery takes — decomposed into measured sub-phases. The
current fault class is replica loss in a Kubernetes-served vLLM
deployment.

Three commitments separate this harness from a load tester:

- **Open loop, coordinated-omission-correct.** Requests dispatch on a
  schedule fixed before the run; the generator never slows down because
  the system did, so a stalled system cannot hide its own stall. Every
  latency is re-based to the request's *intended* dispatch time.
- **Three-state outcomes.** Every scheduled request ends in exactly one
  of completed / errored / censored. Only completions enter latency
  histograms; failure rates are first-class; censored requests enter
  Kaplan-Meier completion curves, never percentiles. A quantile the
  curve does not cross is reported as "beyond the horizon," never
  extrapolated.
- **Run-invalidating self-checks.** The client must prove it was not
  the bottleneck (send-skew, CPU, and GC-pause gates, all pinned and
  run-failing). A run that cannot demonstrate its own validity exits
  invalid instead of publishing.

[SPEC.md](SPEC.md) is the authoritative, pre-registered specification:
every gate, tolerance, and detector parameter carries a pinned number,
enforced at config-load time — a configuration that weakens one refuses
to load. [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) maps each package
to the spec clause it serves and shows how to trace any reported number
back to the line of code that produced it.

## Scope

Phase 0 (complete) builds and certifies the measurement instrument
against a mock OpenAI-compatible SSE server on a local kind cluster:
passing the acceptance suite certifies the measurement machinery, not
any real-GPU claim. Phase 1 characterizes real vLLM on GPU hardware
with the same harness and gates.

## Quickstart (no GPU required)

Requires Go 1.21+, Docker, and [kind](https://kind.sigs.k8s.io/).

```
make test        # the whole gate: unit + AC suite + kind smoke + reproduce
make test-unit   # fast path: unit/integration tests only
make reproduce   # AC7: one-command full harness run against the local cluster
```

## Layout

```
cmd/percentes            single-run harness CLI
cmd/percentes-campaign   N-run campaign runner (SPEC §5 repetition, §10 gates)
cmd/mockserver           fault-injectable mock inference server
internal/                loadgen, collect, detect, orchestrator, validity, ...
configs/                 pinned reference configurations
deploy/                  kind manifests and reproduce scripts
```

## License

Apache-2.0. If you build on the harness or the methodology, cite the
exact commit and configuration of the run you reproduce.
