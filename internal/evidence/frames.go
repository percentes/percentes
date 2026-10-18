package evidence

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
)

// FramesFile is the raw-frame store's file name in a run's frame
// directory.
const FramesFile = "frames.jsonl"

// repoModule is the module path of the instrument's own source tree, where
// the frame store is never written.
const repoModule = "github.com/percentes/percentes"

// FrameRecord is one scheduled request's retained response bytes: the
// timed reads of a 200 body, or the kept start of a non-200 body, with
// credentials masked, and the header values the attempts file withholds.
type FrameRecord struct {
	Index  int64 `json:"index"`
	Status int   `json:"status,omitempty"`
	// Headers are the values of the response headers
	// target.retained_headers keeps out of the attempts file.
	Headers map[string][]string `json:"headers,omitempty"`
	Frames  []Frame             `json:"frames,omitempty"`
	// DroppedBytes counts 200 body bytes read and not kept.
	DroppedBytes int64  `json:"dropped_bytes,omitempty"`
	ErrorBody    *Bytes `json:"error_body,omitempty"`
}

// Frame is one read of a 200 body; Ns is its return time as a nanosecond
// offset from the run epoch, the clock of the attempts file.
type Frame struct {
	Ns int64 `json:"ns"`
	Bytes
}

// Bytes holds raw bytes as Text when they are valid UTF-8, and as Base64
// otherwise.
type Bytes struct {
	Text   string `json:"text,omitempty"`
	Base64 []byte `json:"base64,omitempty"`
}

func bytesOf(b []byte) Bytes {
	if utf8.Valid(b) {
		return Bytes{Text: string(b)}
	}
	return Bytes{Base64: b}
}

// Raw returns the bytes held.
func (b Bytes) Raw() []byte {
	if b.Base64 != nil {
		return b.Base64
	}
	return []byte(b.Text)
}

// FrameRecords returns one record per request of res, in index order.
// Without output, each 200 body's reads are counted in DroppedBytes and
// not kept.
func FrameRecords(res *loadgen.Result, output bool) []FrameRecord {
	out := make([]FrameRecord, len(res.Requests))
	for i := range res.Requests {
		r := &res.Requests[i]
		rec := FrameRecord{Index: r.Index, Status: r.Status, Headers: r.RetainedHeaders, DroppedBytes: r.FrameBytesDropped}
		for _, f := range r.Frames {
			if !output {
				rec.DroppedBytes += int64(len(f.Data))
				continue
			}
			rec.Frames = append(rec.Frames, Frame{Ns: f.Ns, Bytes: bytesOf(f.Data)})
		}
		if len(r.ErrorBody) > 0 {
			b := bytesOf(r.ErrorBody)
			rec.ErrorBody = &b
		}
		out[i] = rec
	}
	return out
}

// WriteFrames writes recs to w, one JSON object per line.
func WriteFrames(w io.Writer, recs []FrameRecord) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for i := range recs {
		if err := enc.Encode(&recs[i]); err != nil {
			return fmt.Errorf("evidence: frames %d: %w", recs[i].Index, err)
		}
	}
	return nil
}

// FramesDir is the run's frame directory under root, named by the run
// epoch in UTC.
func FramesDir(root string, epoch time.Time) string {
	return filepath.Join(root, epoch.UTC().Format("20060102T150405.000000000Z"))
}

// WriteRunFrames writes the frame records of res, keeping the 200 bodies
// where target t keeps model output, to FramesFile in the run's frame
// directory under root, readable by the owner only, and returns the
// file's path. It refuses a root inside the instrument's source tree and
// an existing file. A nil res writes nothing and returns "".
func WriteRunFrames(root string, t *config.Target, res *loadgen.Result) (string, error) {
	if res == nil {
		return "", nil
	}
	if err := CheckFramesRoot(root); err != nil {
		return "", err
	}
	dir := FramesDir(root, res.EpochWall)
	var b bytes.Buffer
	if err := WriteFrames(&b, FrameRecords(res, t.KeepsModelOutput())); err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("evidence: %w", err)
	}
	path := filepath.Join(dir, FramesFile)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("evidence: %w", err)
	}
	if _, err := f.Write(b.Bytes()); err != nil {
		f.Close()
		return "", fmt.Errorf("evidence: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("evidence: %w", err)
	}
	return path, nil
}

// CheckFramesRoot reports an error when root is empty or inside the
// instrument's source tree.
func CheckFramesRoot(root string) error {
	if root == "" {
		return errors.New("evidence: no frame store directory")
	}
	tree, err := sourceTree(root)
	if err != nil {
		return fmt.Errorf("evidence: frame store: %w", err)
	}
	if tree != "" {
		return fmt.Errorf("evidence: frame store %s is inside the instrument source tree %s", root, tree)
	}
	return nil
}

// sourceTree returns the directory at or above dir, after symbolic links
// in its existing part are resolved, whose go.mod declares repoModule, or
// "" when there is none.
func sourceTree(dir string) (string, error) {
	p, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	rest := ""
	for {
		if r, err := filepath.EvalSymlinks(p); err == nil {
			p = filepath.Join(r, rest)
			break
		}
		parent := filepath.Dir(p)
		if parent == p {
			break
		}
		rest = filepath.Join(filepath.Base(p), rest)
		p = parent
	}
	for q := p; ; q = filepath.Dir(q) {
		if b, err := os.ReadFile(filepath.Join(q, "go.mod")); err == nil && modulePath(b) == repoModule {
			return q, nil
		}
		if filepath.Dir(q) == q {
			return "", nil
		}
	}
}

// modulePath returns the path a go.mod's module directive declares.
func modulePath(gomod []byte) string {
	for _, line := range strings.Split(string(gomod), "\n") {
		if f := strings.Fields(line); len(f) >= 2 && f[0] == "module" {
			return strings.Trim(f[1], "\"`")
		}
	}
	return ""
}
