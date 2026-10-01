# Phase 1 deployment

`vllm.yaml` is the SPEC.md §10 Phase 1 topology: two vLLM replicas with
required anti-affinity across two GPU worker nodes, no liveness probe,
prefix caching off. Every `PIN-AT-PHASE1` value is a pre-registration
placeholder, pinned to measured values at hardware bring-up per SPEC.md
§6; the manifest is deliberately not deployable as-is.

## Capture scripts

Three scripts record what SPEC.md §5, §6 and §10 ask for around a run,
each writing one file to keep with the run's reports. `fingerprint.sh`
and `gpusample.sh` run on the GPU host, over `ssh` from the client;
`serverlog.sh` runs wherever the Docker daemon or the named kubectl
context is reachable.

- `fingerprint.sh OUT [CONTAINER]`: the nvidia-smi clock and power state,
  CPU model and core count, kernel, memory and, when CONTAINER is given,
  the container's image id and digest. Run once before and once after
  every run: §6 asks for this fingerprint per replica per run. The §10
  G5 gate does not read these files and reports not applicable.
- `gpusample.sh OUT [DURATION_SECONDS]`: one CSV row a second of pstate,
  clocks, temperature, power draw, utilization and active throttle
  reasons (§10 G5), for the duration or until killed. Over `ssh`, give it
  a duration longer than the run's phases (`warmup_s`, `baseline_s`,
  `fault_window_timeout_s` and `cooldown_s` in the config add up to 1020 s
  in the experiment profile) and background the `ssh` on the client:
  `ssh ubuntu@GPU_HOST percentes/deploy/phase1/gpusample.sh gpu-samples.csv 1100 &`.
  In a shell on the host, start it before the run as a plain background
  command and stop it after:

  ```
  deploy/phase1/gpusample.sh gpu-samples.csv &
  SAMPLER=$!
  # the run
  kill "$SAMPLER"
  ```

- `serverlog.sh OUT docker CONTAINER [SINCE]` or
  `serverlog.sh OUT kube CONTEXT NAMESPACE POD [SINCE]`: the server log
  with timestamps (§5), from SINCE (a timestamp such as
  2026-09-29T12:00:00Z) or from the start.
  Run after the run.
