// Package vllmlog reads the restart boundaries out of a vLLM server log
// (SPEC.md §5, process kill): one pinned pattern per boundary line, the
// last match per pattern at or after the fire, with the line's timestamp
// and the figures vLLM printed on it.
package vllmlog

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Key names one boundary line.
type Key string

// Boundary keys, in the order a vLLM 0.29.0 start prints them.
const (
	KeyBanner          Key = "banner"
	KeyEngineInit      Key = "engine_init"
	KeyModelLoadStart  Key = "model_load_start"
	KeyWeightDownload  Key = "weight_download"
	KeyWeightsLoaded   Key = "weights_loaded"
	KeyModelLoaded     Key = "model_loaded"
	KeyCompileCacheDir Key = "compile_cache_dir"
	KeyDynamo          Key = "dynamo"
	KeyGraphCompile    Key = "graph_compile"
	KeyTorchCompile    Key = "torch_compile"
	KeyGraphCapture    Key = "graph_capture"
	KeyEngineReady     Key = "engine_ready"
	KeyServerStart     Key = "server_start"
	KeyStartupComplete Key = "startup_complete"
)

// Pattern is one pinned boundary line; Figures names the captured numbers in order.
type Pattern struct {
	Key     Key
	Re      *regexp.Regexp
	Figures []string
}

// Match is the last line matching a pattern at or after since.
type Match struct {
	Key        Key                `json:"key"`
	Line       int                `json:"line"`
	At         time.Time          `json:"at,omitempty"`
	Stamp      string             `json:"stamp"` // "docker" | "vllm" | "none"
	Resolution time.Duration      `json:"resolution_ns"`
	Figures    map[string]float64 `json:"figures,omitempty"`
	Text       string             `json:"text"`
}

// Boundaries is the result of one Parse: matches by key and the line count.
type Boundaries struct {
	Matches map[Key]Match `json:"matches"`
	Lines   int           `json:"lines"`
}

// Stamp kinds recorded on a Match.
const (
	StampDocker = "docker"
	StampVLLM   = "vllm"
	StampNone   = "none"
)

// vllmStamp is the level and MM-DD HH:MM:SS prefix of a vLLM logger line.
var vllmStamp = regexp.MustCompile(`(?:DEBUG|INFO|WARNING|ERROR|CRITICAL) (\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2}) \[`)

// Parse keeps, per key, the last match at or after since; year completes a
// bare vLLM stamp (MM-DD HH:MM:SS, 1 s resolution). A Docker stamp
// (RFC 3339 with nanoseconds at the start of the line) takes precedence.
// A line with no stamp is kept when it follows a kept stamped line.
func Parse(b []byte, pats []Pattern, since time.Time, year int) Boundaries {
	lines := strings.Split(string(b), "\n")
	if n := len(lines); n > 0 && lines[n-1] == "" {
		lines = lines[:n-1]
	}
	out := Boundaries{Matches: map[Key]Match{}, Lines: len(lines)}
	kept := since.IsZero()
	for i, line := range lines {
		at, stamp, res := stampOf(line, year)
		switch stamp {
		case StampDocker:
			kept = since.IsZero() || !at.Before(since)
		case StampVLLM:
			kept = since.IsZero() || !at.Before(since.Truncate(time.Second))
		}
		if !kept {
			continue
		}
		for _, p := range pats {
			sub := p.Re.FindStringSubmatch(line)
			if sub == nil {
				continue
			}
			m := Match{Key: p.Key, Line: i + 1, At: at, Stamp: stamp, Resolution: res, Text: strings.TrimSuffix(line, "\r")}
			for j, name := range p.Figures {
				if j+1 >= len(sub) {
					break
				}
				v, err := strconv.ParseFloat(sub[j+1], 64)
				if err != nil {
					continue
				}
				if m.Figures == nil {
					m.Figures = map[string]float64{}
				}
				m.Figures[name] = v
			}
			out.Matches[p.Key] = m
		}
	}
	return out
}

// stampOf reads a line's Docker stamp, else its vLLM stamp. vLLM stamps
// carry no zone and are read as UTC, the container's default.
func stampOf(line string, year int) (time.Time, string, time.Duration) {
	if sp := strings.IndexByte(line, ' '); sp > 0 {
		if t, err := time.Parse(time.RFC3339Nano, line[:sp]); err == nil {
			return t, StampDocker, time.Nanosecond
		}
	}
	if sub := vllmStamp.FindStringSubmatch(line); sub != nil {
		n := make([]int, 5)
		for i := range n {
			n[i], _ = strconv.Atoi(sub[i+1])
		}
		t := time.Date(year, time.Month(n[0]), n[1], n[2], n[3], n[4], 0, time.UTC)
		return t, StampVLLM, time.Second
	}
	return time.Time{}, StampNone, 0
}
