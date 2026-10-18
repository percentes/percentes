package loadgen

import (
	"bytes"
	"io"
	"testing"
)

// chunkReader returns one chunk per Read.
type chunkReader struct{ chunks []string }

func (c *chunkReader) Read(p []byte) (int, error) {
	if len(c.chunks) == 0 {
		return 0, io.EOF
	}
	n := copy(p, c.chunks[0])
	c.chunks = c.chunks[1:]
	return n, nil
}

// Reads are kept with their times up to the limit; the read that would
// pass it and every later read are counted, and a kept tail that begins
// a secret is masked.
func TestFrameRecorderLimit(t *testing.T) {
	var clock int64
	f := &frameRecorder{
		r:     &chunkReader{chunks: []string{"abcd", "efgh", "ijkl", "mn"}},
		now:   func() int64 { clock += 5; return clock },
		limit: 10,
	}
	got, err := io.ReadAll(f)
	if err != nil || string(got) != "abcdefghijklmn" {
		t.Fatalf("reader passed %q, %v", got, err)
	}
	frames, dropped := f.result([]string{"ghijkl"})
	if dropped != 6 || len(frames) != 2 {
		t.Fatalf("%d frames, %d dropped; want 2 and 6", len(frames), dropped)
	}
	for i, want := range []Frame{{Ns: 5, Data: []byte("abcd")}, {Ns: 10, Data: []byte("ef**")}} {
		if frames[i].Ns != want.Ns || !bytes.Equal(frames[i].Data, want.Data) {
			t.Errorf("frame %d = %d %q, want %d %q", i, frames[i].Ns, frames[i].Data, want.Ns, want.Data)
		}
	}
}

// A secret split across two reads is masked in both frames.
func TestFrameRecorderMasksAcrossReads(t *testing.T) {
	f := &frameRecorder{
		r:     &chunkReader{chunks: []string{"x sk-sec", "ret-key y"}},
		now:   func() int64 { return 1 },
		limit: 1 << 10,
	}
	if _, err := io.ReadAll(f); err != nil {
		t.Fatal(err)
	}
	frames, _ := f.result([]string{"sk-secret-key"})
	if string(frames[0].Data) != "x ******" || string(frames[1].Data) != "******* y" {
		t.Fatalf("frames %q %q", frames[0].Data, frames[1].Data)
	}
}

// A hosted 200 body is kept as frames; a self-hosted one is not.
func TestFramesHostedOnly(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"
	srv := sseServer(t, body)
	defer srv.Close()

	g := captureGen(srv)
	r := &Request{Index: 1}
	g.execute(r)
	var got []byte
	for _, f := range r.Frames {
		if f.Ns < r.DispatchNs {
			t.Errorf("frame at %d before dispatch at %d", f.Ns, r.DispatchNs)
		}
		got = append(got, f.Data...)
	}
	if r.Outcome != OutcomeCompleted || string(got) != body {
		t.Fatalf("hosted: %v, frames %q", r.Outcome, got)
	}

	g.cfg.Target.Hosted = false
	r = &Request{Index: 2}
	g.execute(r)
	if r.Outcome != OutcomeCompleted || r.Frames != nil {
		t.Fatalf("self-hosted: %v, %d frames", r.Outcome, len(r.Frames))
	}
}
