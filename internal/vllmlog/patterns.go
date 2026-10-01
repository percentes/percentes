package vllmlog

import "regexp"

// VLLM0290 is the boundary line set of a vLLM 0.29.0 start, taken from the
// 16 September 2026 calibration log (testdata/vllm-startup-16Sep2026.log).
var VLLM0290 = []Pattern{
	{Key: KeyBanner, Re: regexp.MustCompile(`\[api_utils\.py:\d+\] .*version 0\.29\.0`)},
	{Key: KeyEngineInit, Re: regexp.MustCompile(`\[core\.py:\d+\] Initializing a V1 LLM engine \(v0\.29\.0\)`)},
	{Key: KeyModelLoadStart, Re: regexp.MustCompile(`\[model_runner\.py:\d+\] Loading model from scratch\.\.\.`)},
	{Key: KeyWeightDownload, Re: regexp.MustCompile(`Time spent downloading weights for .*: ([0-9.]+) seconds`), Figures: []string{"weight_download_s"}},
	{Key: KeyWeightsLoaded, Re: regexp.MustCompile(`\[default_loader\.py:\d+\] Loading weights took ([0-9.]+) seconds`), Figures: []string{"weights_loaded_s"}},
	{Key: KeyModelLoaded, Re: regexp.MustCompile(`Model loading took ([0-9.]+) GiB memory and ([0-9.]+) seconds`), Figures: []string{"model_loaded_gib", "model_loaded_s"}},
	{Key: KeyCompileCacheDir, Re: regexp.MustCompile(`Using cache directory: (\S+) for vLLM's torch\.compile`)},
	{Key: KeyDynamo, Re: regexp.MustCompile(`Dynamo bytecode transform time: ([0-9.]+) s`), Figures: []string{"dynamo_s"}},
	{Key: KeyGraphCompile, Re: regexp.MustCompile(`Compiling a graph for compile range .* takes ([0-9.]+) s`), Figures: []string{"graph_compile_s"}},
	{Key: KeyTorchCompile, Re: regexp.MustCompile(`\[monitor\.py:\d+\] torch\.compile took ([0-9.]+) s in total`), Figures: []string{"torch_compile_s"}},
	{Key: KeyGraphCapture, Re: regexp.MustCompile(`Graph capturing finished in ([0-9.]+) secs, took ([0-9.]+) GiB`), Figures: []string{"graph_capture_s", "graph_capture_gib"}},
	{Key: KeyEngineReady, Re: regexp.MustCompile(`init engine \(profile, create kv cache, warmup model\) took ([0-9.]+) s \(compilation: ([0-9.]+) s\)`), Figures: []string{"init_engine_s", "init_engine_compilation_s"}},
	{Key: KeyServerStart, Re: regexp.MustCompile(`\[entry\.py:\d+\] Starting vLLM server on`)},
	{Key: KeyStartupComplete, Re: regexp.MustCompile(`Application startup complete\.`)},
}

// Mock is the mock server's one boundary line.
var Mock = []Pattern{{Key: KeyServerStart, Re: regexp.MustCompile(`mockserver: serving on`)}}
