#!/usr/bin/env bash
# The server log for a run window, with timestamps, to OUT: a Docker
# container's log, or a pod's log through kubectl with the context named
# (SPEC.md §5: the weight-load and CUDA-graph boundaries are log-derived).
set -euo pipefail

usage() {
  cat >&2 <<USAGE
usage: $0 OUT docker CONTAINER [SINCE]
       $0 OUT kube CONTEXT NAMESPACE POD [SINCE]
SINCE is an RFC 3339 time; without it the whole log is saved.
USAGE
  exit 2
}
[ $# -ge 3 ] || usage
OUT=$1
MODE=$2
shift 2

case $MODE in
  docker)
    [ $# -ge 1 ] || usage
    if [ $# -ge 2 ]; then
      docker logs --timestamps --since "$2" "$1" > "$OUT" 2>&1
    else
      docker logs --timestamps "$1" > "$OUT" 2>&1
    fi
    ;;
  kube)
    [ $# -ge 3 ] || usage
    if [ $# -ge 4 ]; then
      kubectl --context "$1" -n "$2" logs "$3" --timestamps --since-time="$4" > "$OUT"
    else
      kubectl --context "$1" -n "$2" logs "$3" --timestamps > "$OUT"
    fi
    ;;
  *) usage ;;
esac
echo "$OUT"
