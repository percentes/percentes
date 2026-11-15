package main

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/percentes/percentes/internal/evidence"
	"github.com/shirou/gopsutil/v3/mem"
)

// rehearsalResources are what one rehearsal cell used of the host,
// against the bounds the rehearsal declares: the process's peak resident
// set size beside the host's memory; the most open descriptors; the most
// open and TIME_WAIT sockets to one destination beside the local port
// range, read from /proc/net where the host has it (SocketsUnread says
// why not otherwise), and the host's tcp_tw_reuse setting; the cell's
// bytes on disk, its attempts and frames files among them; and the delay
// from dispatch to request written over the cell's requests.
type rehearsalResources struct {
	PeakRSSBytes     uint64          `json:"peak_rss_bytes"`
	MemoryBytes      uint64          `json:"memory_bytes"`
	PeakOpenFiles    int             `json:"peak_open_files"`
	PeakSockets      int             `json:"peak_sockets_per_destination,omitempty"`
	PeakDestination  string          `json:"peak_destination,omitempty"`
	PeakTimeWait     int             `json:"peak_time_wait,omitempty"`
	LocalPortRange   []int           `json:"local_port_range,omitempty"`
	TCPTimeWaitReuse *int            `json:"tcp_tw_reuse,omitempty"`
	SocketsUnread    string          `json:"sockets_unread,omitempty"`
	AttemptsBytes    int64           `json:"attempts_bytes"`
	FramesBytes      int64           `json:"frames_bytes"`
	CellBytes        int64           `json:"cell_bytes"`
	WriteDelay       *rehearsalDelay `json:"dispatch_to_write,omitempty"`
	PeakRSSUnread    string          `json:"peak_rss_unread,omitempty"`
	MemoryUnread     string          `json:"memory_unread,omitempty"`
	OpenFilesUnread  string          `json:"open_files_unread,omitempty"`
}

// rehearsalDelay is the delay from dispatch to request written over a
// cell's dispatched requests: the written ones' median, 99th percentile
// and largest, and the count with no write time.
type rehearsalDelay struct {
	Written   int   `json:"written"`
	P50Ns     int64 `json:"p50_ns"`
	P99Ns     int64 `json:"p99_ns"`
	MaxNs     int64 `json:"max_ns"`
	Unwritten int   `json:"unwritten"`
}

// procNet are the socket tables the sampler reads on Linux.
var procNet = []string{"/proc/net/tcp", "/proc/net/tcp6"}

// portRangeFile holds the local port range on Linux.
const portRangeFile = "/proc/sys/net/ipv4/ip_local_port_range"

// twReuseFile holds the tcp_tw_reuse setting on Linux.
const twReuseFile = "/proc/sys/net/ipv4/tcp_tw_reuse"

// hostSampler reads the open descriptors and the sockets per
// destination once a second until stopped, keeping the peaks.
type hostSampler struct {
	stop chan struct{}
	done chan struct{}

	mu        sync.Mutex
	res       rehearsalResources
	framesPre map[string]int64
}

// sampleHost starts a sampler; framesRoot's frames files are listed so
// that the cell's own can be told from earlier ones.
func sampleHost(framesRoot string) *hostSampler {
	s := &hostSampler{stop: make(chan struct{}), done: make(chan struct{})}
	s.framesPre, _ = framesFiles(framesRoot)
	if _, err := os.Stat(procNet[0]); err != nil {
		s.res.SocketsUnread = fmt.Sprintf("no %s on %s", procNet[0], runtime.GOOS)
	}
	go func() {
		defer close(s.done)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			s.sample()
			select {
			case <-s.stop:
				s.sample()
				return
			case <-t.C:
			}
		}
	}()
	return s
}

func (s *hostSampler) sample() {
	n, ferr := openFiles()
	var dest string
	var socks, tw int
	var serr error
	if s.res.SocketsUnread == "" {
		dest, socks, tw, serr = socketsPerDestination()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if ferr != nil {
		s.res.OpenFilesUnread = ferr.Error()
	} else {
		s.res.PeakOpenFiles = max(s.res.PeakOpenFiles, n)
	}
	switch {
	case serr != nil:
		s.res.SocketsUnread = serr.Error()
	case socks > s.res.PeakSockets:
		s.res.PeakSockets, s.res.PeakDestination = socks, dest
	}
	s.res.PeakTimeWait = max(s.res.PeakTimeWait, tw)
}

// finish stops the sampler and completes its record for the cell in
// dir, whose frames files are under framesRoot.
func (s *hostSampler) finish(dir, framesRoot string) *rehearsalResources {
	close(s.stop)
	<-s.done
	res := s.res
	if rss, err := peakRSS(); err != nil {
		res.PeakRSSUnread = err.Error()
	} else {
		res.PeakRSSBytes = rss
	}
	if vm, err := mem.VirtualMemory(); err != nil {
		res.MemoryUnread = err.Error()
	} else {
		res.MemoryBytes = vm.Total
	}
	res.LocalPortRange, _ = localPortRange()
	if b, err := os.ReadFile(twReuseFile); err == nil {
		if v, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil {
			res.TCPTimeWaitReuse = &v
		}
	}
	if fi, err := os.Stat(filepath.Join(dir, evidence.AttemptsFile)); err == nil {
		res.AttemptsBytes = fi.Size()
	}
	post, _ := framesFiles(framesRoot)
	for p, n := range post {
		if _, ok := s.framesPre[p]; !ok {
			res.FramesBytes += n
		}
	}
	res.CellBytes = treeBytes(dir) + res.FramesBytes
	res.WriteDelay = writeDelay(dir)
	return &res
}

// problems are the bounds res breaks: peak resident set size above half
// the host's memory, open descriptors at the open-file floor or above,
// and sockets to one destination at the local port range's size or
// above. A failed socket or port-range reading is a problem on Linux
// only; a failed resident-set, memory or descriptor reading is a
// problem on every host.
func (res *rehearsalResources) problems() []string {
	var out []string
	linux := runtime.GOOS == "linux"
	switch {
	case res.PeakRSSUnread != "" || res.MemoryUnread != "":
		out = append(out, fmt.Sprintf("peak resident set size or host memory not read: %s%s", res.PeakRSSUnread, res.MemoryUnread))
	case res.PeakRSSBytes > res.MemoryBytes/2:
		out = append(out, fmt.Sprintf("peak resident set size %d bytes is above half the host's %d bytes", res.PeakRSSBytes, res.MemoryBytes))
	}
	switch {
	case res.OpenFilesUnread != "":
		out = append(out, "open descriptors not read: "+res.OpenFilesUnread)
	case res.PeakOpenFiles >= minOpenFiles:
		out = append(out, fmt.Sprintf("%d open descriptors, at or above %d", res.PeakOpenFiles, minOpenFiles))
	}
	switch {
	case res.SocketsUnread != "" || len(res.LocalPortRange) != 2:
		if linux {
			out = append(out, fmt.Sprintf("sockets per destination or the local port range not read: %q", res.SocketsUnread))
		}
	case res.PeakSockets >= res.LocalPortRange[1]-res.LocalPortRange[0]+1:
		out = append(out, fmt.Sprintf("%d sockets to %s, at or above the local port range %v", res.PeakSockets, res.PeakDestination, res.LocalPortRange))
	}
	return out
}

// openFiles counts this process's open descriptors.
func openFiles() (int, error) {
	d, err := os.Open("/dev/fd")
	if err != nil {
		return 0, err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return 0, err
	}
	// Reading the directory holds one descriptor of its own.
	return len(names) - 1, nil
}

// peakRSS is this process's peak resident set size in bytes.
func peakRSS() (uint64, error) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, err
	}
	if runtime.GOOS == "darwin" {
		return uint64(ru.Maxrss), nil
	}
	// Linux reports kibibytes.
	return uint64(ru.Maxrss) * 1024, nil
}

// socketsPerDestination reads the TCP socket tables and returns the
// remote address with the most sockets other than listeners, their
// count, and the sockets in TIME_WAIT.
func socketsPerDestination() (dest string, peak, timeWait int, err error) {
	count := map[string]int{}
	for _, p := range procNet {
		f, err := os.Open(p)
		if err != nil {
			if os.IsNotExist(err) && p != procNet[0] {
				continue
			}
			return "", 0, 0, err
		}
		sc := bufio.NewScanner(f)
		sc.Scan()
		for sc.Scan() {
			fields := strings.Fields(sc.Text())
			if len(fields) < 4 {
				continue
			}
			switch fields[3] {
			case "0A":
				continue
			case "06":
				timeWait++
			}
			count[fields[2]]++
		}
		err = sc.Err()
		f.Close()
		if err != nil {
			return "", 0, 0, err
		}
	}
	keys := make([]string, 0, len(count))
	for k := range count {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		if count[k] > peak {
			dest, peak = k, count[k]
		}
	}
	return dest, peak, timeWait, nil
}

// localPortRange is the host's local port range, low and high.
func localPortRange() ([]int, error) {
	b, err := os.ReadFile(portRangeFile)
	if err != nil {
		return nil, err
	}
	f := strings.Fields(string(b))
	if len(f) != 2 {
		return nil, fmt.Errorf("%s: %q", portRangeFile, b)
	}
	lo, err1 := strconv.Atoi(f[0])
	hi, err2 := strconv.Atoi(f[1])
	if err1 != nil || err2 != nil || hi < lo {
		return nil, fmt.Errorf("%s: %q", portRangeFile, b)
	}
	return []int{lo, hi}, nil
}

// framesFiles are the frames files under root with their sizes.
func framesFiles(root string) (map[string]int64, error) {
	out := map[string]int64{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() != evidence.FramesFile {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		out[p] = fi.Size()
		return nil
	})
	return out, err
}

// treeBytes is the size of the regular files under dir.
func treeBytes(dir string) int64 {
	var n int64
	filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error { //nolint:errcheck
		if err == nil && d.Type().IsRegular() {
			if fi, err := d.Info(); err == nil {
				n += fi.Size()
			}
		}
		return nil
	})
	return n
}

// writeDelay is the delay from dispatch to request written over the
// attempts file in dir; nil where it cannot be read.
func writeDelay(dir string) *rehearsalDelay {
	recs, err := readCellRecords(dir)
	if err != nil {
		return nil
	}
	d := &rehearsalDelay{}
	var ns []int64
	for _, r := range recs {
		switch {
		case r.DispatchNs == 0:
		case r.WroteRequestNs == 0:
			d.Unwritten++
		default:
			ns = append(ns, r.WroteRequestNs-r.DispatchNs)
		}
	}
	if d.Written = len(ns); d.Written > 0 {
		slices.Sort(ns)
		at := func(q float64) int64 { return ns[min(len(ns)-1, int(q*float64(len(ns))))] }
		d.P50Ns, d.P99Ns, d.MaxNs = at(0.5), at(0.99), ns[len(ns)-1]
	}
	return d
}

// readCellRecords reads the attempts file in a cell's directory.
func readCellRecords(dir string) ([]evidence.Record, error) {
	f, err := os.Open(filepath.Join(dir, evidence.AttemptsFile))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return evidence.ReadRecords(f)
}
