package run

import (
	"github.com/percentes/percentes/internal/orchestrator"
	"github.com/percentes/percentes/internal/vllmlog"
)

// ContainerRestart is the process-kill record of one run: the container
// before and after, the clock offsets, the kill bracket, the runtime's
// die and start events and the restart boundaries read from the server
// log. Host times are as the host recorded them; the offsets convert them.
// FireUncertaintyNs is orchestrator.FireUncertaintyOf the kill with both
// offsets.
type ContainerRestart struct {
	Before            orchestrator.ContainerState `json:"before"`
	After             orchestrator.ContainerState `json:"after"`
	OffsetAtArm       orchestrator.ClockOffset    `json:"offset_at_arm"`
	OffsetAfter       orchestrator.ClockOffset    `json:"offset_after"`
	Kill              *orchestrator.KillRecord    `json:"kill,omitempty"`
	FireUncertaintyNs int64                       `json:"fire_uncertainty_ns"`
	// An in-flight request ending in [fire, fire+IndeterminateZoneNs] is
	// indeterminate: the fire uncertainty plus the larger offset bound as
	// the allowance for delivery to the client.
	IndeterminateZoneNs int64                         `json:"indeterminate_zone_ns"`
	Events              []orchestrator.ContainerEvent `json:"events,omitempty"`
	DieToStartS         *float64                      `json:"die_to_start_s,omitempty"`
	Boundaries          vllmlog.Boundaries            `json:"boundaries"`
	LogPath             string                        `json:"log_path"`
	LogBytes            int                           `json:"log_bytes"`
	FingerprintBefore   string                        `json:"fingerprint_before_path"`
	FingerprintAfter    string                        `json:"fingerprint_after_path"`
}
