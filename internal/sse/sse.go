// Package sse frames a Server-Sent Events stream per the HTML Standard's
// line terminators and data-field joining, with a leading byte-order mark
// allowed. An event whose data exceeds the limit is dropped and counted,
// and a single line over the limit ends the scan with bufio.ErrTooLong,
// counted the same way. An event with empty data is not delivered, and a
// line reading only "data" is ignored.
package sse

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

// SplitLines is a bufio.SplitFunc for the three line terminators.
func SplitLines(data []byte, atEOF bool) (int, []byte, error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	for i := 0; i < len(data); i++ {
		switch data[i] {
		case '\n':
			return i + 1, data[:i], nil
		case '\r':
			if i+1 < len(data) {
				if data[i+1] == '\n' {
					return i + 2, data[:i], nil
				}
				return i + 1, data[:i], nil
			}
			if atEOF {
				return i + 1, data[:i], nil
			}
			// A trailing carriage return may yet be followed by a line feed.
			return 0, nil, nil
		}
	}
	if atEOF {
		return len(data), data, nil
	}
	return 0, nil, nil
}

// Events delivers each event's joined data to fn until fn returns true or
// the stream ends. It reports whether any data field was seen, how many
// events were dropped for exceeding limit, and the read error if one ended
// the stream. A final event without a trailing blank line is still
// delivered. payload is valid only for the duration of the call.
func Events(r io.Reader, limit int, fn func(payload []byte) (stop bool)) (sawData bool, dropped int, err error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 4096), limit)
	sc.Split(SplitLines)
	var event []byte
	overflow, firstLine := false, true
	for sc.Scan() {
		line := sc.Bytes()
		if firstLine {
			line = bytes.TrimPrefix(line, []byte("\xef\xbb\xbf"))
			firstLine = false
		}
		if len(line) == 0 {
			payload := bytes.TrimSuffix(event, []byte("\n"))
			stop := !overflow && len(payload) > 0 && fn(payload)
			event, overflow = event[:0], false
			if stop {
				return sawData, dropped, nil
			}
			continue
		}
		v, ok := bytes.CutPrefix(line, []byte("data:"))
		if !ok {
			continue
		}
		sawData = true
		if overflow {
			continue
		}
		v = bytes.TrimPrefix(v, []byte(" "))
		if len(event)+len(v)+1 > limit {
			dropped++
			event, overflow = event[:0], true
			continue
		}
		event = append(append(event, v...), '\n')
	}
	if payload := bytes.TrimSuffix(event, []byte("\n")); !overflow && len(payload) > 0 {
		fn(payload)
	}
	err = sc.Err()
	if errors.Is(err, bufio.ErrTooLong) {
		dropped++
	}
	return sawData, dropped, err
}
