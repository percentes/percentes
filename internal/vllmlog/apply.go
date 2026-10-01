package vllmlog

import (
	"fmt"
	"time"

	"github.com/percentes/percentes/internal/detect"
)

// logRow is one log-derived segment; an empty from is the fire.
type logRow struct {
	name     string
	from, to Key
	absent   string // note when to is absent and from is present
}

var logRows = []logRow{
	{name: "log_bringup", from: KeyBanner, to: KeyServerStart},
	{name: "engine_init", to: KeyEngineInit},
	{name: "weight_download", from: KeyModelLoadStart, to: KeyWeightDownload,
		absent: "no download line after the fire: weights served from the mounted cache"},
	{name: "weight_load", from: KeyModelLoadStart, to: KeyWeightsLoaded},
	{name: "torch_compile", from: KeyModelLoaded, to: KeyTorchCompile},
	{name: "profile_kv_capture", from: KeyTorchCompile, to: KeyGraphCapture},
	{name: "engine_ready", to: KeyEngineReady},
	{name: "server_ready", to: KeyStartupComplete},
}

// Apply fills the log rows of d (detect.NewDecomposition) from b; offset is host
// minus client and is subtracted from every At. An absent key leaves
// its row N/A with "pattern <key> not found in the log after the fire".
func Apply(d *detect.Decomposition, b Boundaries, fire time.Time, offset time.Duration) {
	point := func(k Key) (time.Time, string) {
		m, ok := b.Matches[k]
		if !ok {
			return time.Time{}, fmt.Sprintf("pattern %s not found in the log after the fire", k)
		}
		if m.At.IsZero() {
			return time.Time{}, fmt.Sprintf("pattern %s found on line %d with no Docker stamp", k, m.Line)
		}
		return m.At.Add(-offset), ""
	}
	for _, r := range logRows {
		start, note := fire, ""
		if r.from != "" {
			start, note = point(r.from)
		}
		var end time.Time
		if note == "" {
			end, note = point(r.to)
			if _, ok := b.Matches[r.to]; !ok && r.absent != "" {
				note = r.absent
			}
		}
		if note != "" {
			d.SetNote(r.name, note)
			continue
		}
		d.SetMeasured(r.name, start, end)
		for i := range d.Segments {
			if d.Segments[i].Name == r.name && d.Segments[i].Measured {
				d.Segments[i].Note = ""
			}
		}
	}
	for _, m := range b.Matches {
		for name, v := range m.Figures {
			if d.LogFigures == nil {
				d.LogFigures = map[string]float64{}
			}
			d.LogFigures[name] = v
		}
	}
}
