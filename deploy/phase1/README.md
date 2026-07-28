# Phase 1 deployment

`vllm.yaml` is the SPEC.md §10 Phase 1 topology: two vLLM replicas with
required anti-affinity across two GPU worker nodes, no liveness probe,
prefix caching off. Every `PIN-AT-PHASE1` value is a pre-registration
placeholder, pinned to measured values at hardware bring-up per SPEC.md
§6; the manifest is deliberately not deployable as-is.
