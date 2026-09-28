#!/usr/bin/env bash
# Host and GPU fingerprint for one replica host (SPEC.md §6): the
# nvidia-smi clock and power state, CPU model and core count, kernel,
# memory, and the container's image id and digest when a container name
# is given. Written to OUT under a UTC header. Run before and after a run.
set -euo pipefail

usage() { echo "usage: $0 OUT [CONTAINER]" >&2; exit 2; }
[ $# -ge 1 ] || usage
OUT=$1
CONTAINER=${2:-}

GPU_FIELDS=name,uuid,driver_version,vbios_version,persistence_mode,pstate,clocks.sm,clocks.mem,clocks.max.sm,clocks.max.mem,power.limit,power.max_limit,temperature.gpu,clocks_throttle_reasons.active

{
  printf '# fingerprint %s %s\n' "$(hostname)" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '== nvidia-smi --query-gpu=%s ==\n' "$GPU_FIELDS"
  nvidia-smi --query-gpu="$GPU_FIELDS" --format=csv
  printf '== lscpu ==\n'
  lscpu
  printf '== nproc ==\n'
  nproc
  printf '== uname -a ==\n'
  uname -a
  printf '== /proc/meminfo, first three lines ==\n'
  head -n 3 /proc/meminfo
  if [ -n "$CONTAINER" ]; then
    printf '== container %s ==\n' "$CONTAINER"
    docker inspect --format 'image {{.Config.Image}}' "$CONTAINER"
    docker inspect --format 'started {{.State.StartedAt}}' "$CONTAINER"
    docker image inspect --format 'id {{.Id}}{{"\n"}}digest {{if .RepoDigests}}{{index .RepoDigests 0}}{{else}}none{{end}}' "$(docker inspect --format '{{.Image}}' "$CONTAINER")"
  fi
} > "$OUT"
echo "$OUT"
