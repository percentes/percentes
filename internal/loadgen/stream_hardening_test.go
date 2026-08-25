package loadgen

import "testing"

// A non-JSON payload on a data: line is a malformed stream (§3), never
// a countable token.
func TestMalformedChunkErrored(t *testing.T) {
	srv := sseServer(t, "data: {\"content\": not-json\n\n", "data: [DONE]\n\n")
	defer srv.Close()

	r := executeAgainst(srv)
	if r.Outcome != OutcomeErrored || r.ErrClass != ErrMalformedStream {
		t.Fatalf("malformed chunk must be errored/%s, got %v/%q", ErrMalformedStream, r.Outcome, r.ErrClass)
	}
	if r.Tokens != 0 {
		t.Fatalf("malformed chunk must not count as a token, got %d", r.Tokens)
	}
}

// A role-style chunk with an empty content delta carries no token and
// must not stamp TTFT.
func TestEmptyContentDeltaNotAToken(t *testing.T) {
	srv := sseServer(t,
		"data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{\"content\":\"tok\"}}]}\n\n",
		"data: [DONE]\n\n")
	defer srv.Close()

	r := executeAgainst(srv)
	if r.Outcome != OutcomeCompleted {
		t.Fatalf("want completed, got %v/%q", r.Outcome, r.ErrClass)
	}
	if r.Tokens != 1 {
		t.Fatalf("empty content delta must not count: want 1 token, got %d", r.Tokens)
	}
}

// [DONE] is an exact terminator; trailing bytes make the stream malformed.
func TestDoneWithTrailingJunkErrored(t *testing.T) {
	srv := sseServer(t,
		"data: {\"choices\":[{\"delta\":{\"content\":\"tok\"}}]}\n\n",
		"data: [DONE]garbage\n\n")
	defer srv.Close()

	r := executeAgainst(srv)
	if r.Outcome != OutcomeErrored || r.ErrClass != ErrMalformedStream {
		t.Fatalf("trailing junk after [DONE] must be errored/%s, got %v/%q", ErrMalformedStream, r.Outcome, r.ErrClass)
	}
}

// The mock's own frame shape is a completion.
func TestMockShapedFramesStillComplete(t *testing.T) {
	var frames []string
	for i := 0; i < 3; i++ {
		frames = append(frames, "data: {\"id\":\"cmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"tok \"},\"finish_reason\":null}]}\n\n")
	}
	frames = append(frames, "data: [DONE]\n\n")
	srv := sseServer(t, frames...)
	defer srv.Close()

	r := executeAgainst(srv)
	if r.Outcome != OutcomeCompleted || r.Tokens != 3 {
		t.Fatalf("want completed with 3 tokens, got %v/%q tokens=%d", r.Outcome, r.ErrClass, r.Tokens)
	}
}
