#!/usr/bin/env bash
# One CSV row a second of the GPU's pstate, clocks, temperature, power
# draw, utilization and active throttle reasons, to OUT (SPEC.md §6 and
# §10 G5: GPU clock and power policy equal across replicas and runs). For
# DURATION seconds when given, else until killed; killing the script
# ends the sampling. The first query is checked before the loop.
set -euo pipefail

usage() { echo "usage: $0 OUT [DURATION_SECONDS]" >&2; exit 2; }
[ $# -ge 1 ] || usage
OUT=$1
DURATION=${2:-}

FIELDS=timestamp,index,pstate,clocks.sm,clocks.mem,temperature.gpu,power.draw,utilization.gpu,utilization.memory,clocks_throttle_reasons.active

nvidia-smi --query-gpu="$FIELDS" --format=csv > "$OUT"
if [ -n "$DURATION" ]; then
  timeout "$DURATION" nvidia-smi --query-gpu="$FIELDS" --format=csv,noheader -l 1 >> "$OUT" &
  CHILD=$!
  trap 'kill "$CHILD" 2>/dev/null' TERM INT
  wait "$CHILD" || [ $? -eq 124 ]
else
  exec nvidia-smi --query-gpu="$FIELDS" --format=csv,noheader -l 1 >> "$OUT"
fi
