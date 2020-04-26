// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package redact

import (
	"bytes"
	"fmt"
	"io"
)

// escapeState abstracts on top of fmt.State and ensures that call
// calls to Write() enclose the writtne bytes between unsafe markers.
type escapeState struct {
	fmt.State
	w escapeWriter
}

var _ fmt.State = (*escapeState)(nil)

func makeEscapeState(s fmt.State, buf *bytes.Buffer) escapeState {
	e := escapeState{State: s}
	e.w = escapeWriter{w: buf, enclose: true}
	return e
}

// Write is part of the fmt.State interface and implements the io.Writer interface.
func (p *escapeState) Write(b []byte) (int, error) {
	return p.w.Write(b)
}

// escapeWriter abstracts on top of io.Writer and ensures that all
// calls to Write() escape markers.
// Also, final spaces and newlines are stripped if strip is set.
// Also, the overall result is enclosed inside redaction markers
// if enclose is true.
type escapeWriter struct {
	w       io.Writer
	enclose bool
	strip   bool
}

var _ io.Writer = (*escapeWriter)(nil)

// Write implements the io.Writer interface.
func (p *escapeWriter) Write(b []byte) (int, error) {
	st := escapeResult{0, nil}

	if p.strip {
		// Trim final newlines/spaces, for convenience.
		end := len(b)
		for i := end - 1; i >= 0; i-- {
			if b[i] == '\n' || b[i] == ' ' {
				end = i
			} else {
				break
			}
		}
		b = b[:end]
	}

	// Here we could choose to omit the output
	// entirely if there was nothing but empty space:
	// if len(b) == 0 { return 0, nil }

	start := startRedactableBytes
	ls := len(startRedactableS)
	end := endRedactableBytes
	le := len(endRedactableS)
	escape := escapeBytes

	if p.enclose {
		st = p.doWrite(start, st, false)
	}

	// Now write the string.
	k := 0
	for i := 0; i < len(b); i++ {
		// Ensure that occurrences of the delimiter inside the string get
		// escaped.
		if i+ls <= len(b) && bytes.Equal(b[i:i+ls], start) {
			st = p.doWrite(b[k:i], st, true)
			st = p.doWrite(escape, st, false)
			st.l += ls
			k = i + ls
			i += ls - 1
		} else if i+le <= len(b) && bytes.Equal(b[i:i+le], end) {
			st = p.doWrite(b[k:i], st, true)
			st = p.doWrite(escape, st, false)
			st.l += le
			k = i + le
			i += le - 1
		}
	}
	st = p.doWrite(b[k:], st, true)
	if p.enclose {
		st = p.doWrite(end, st, false)
	}
	return st.l, st.err
}

type escapeResult struct {
	l   int
	err error
}

func (p *escapeWriter) doWrite(b []byte, st escapeResult, count bool) escapeResult {
	if st.err != nil {
		// An error was encountered previously.
		// No-op.
		return st
	}
	sz, err := p.w.Write(b)
	if count {
		st.l += sz
	}
	st.err = err
	return st
}
