package main

// hostGatesOpen widens the client-validity gate's GC pause and CPU bounds,
// which read this test process, so the cell's label rests on its ladder.
func hostGatesOpen(r *route) {
	r.cfg.ClientValidity.GoGCPauseP99Ms = 1000
	r.cfg.ClientValidity.MaxCPUPct = 100
}

// hostGatesWide widens hostGatesOpen's bounds and the send-skew bounds,
// so the cell's label rests on its ladder and its undispatched requests.
func hostGatesWide(r *route) {
	hostGatesOpen(r)
	r.cfg.ClientValidity.SendSkewP99Ms, r.cfg.ClientValidity.SendSkewMaxMs = 1000, 1000
}
