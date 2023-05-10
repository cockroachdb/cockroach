// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package copy

import (
	"bytes"
	"io"
)

const chunkSize = 64 << 10

type BufferingWriter struct {
	// Limit at which we'll stop buffering.
	Limit int64
	Grow  func(int) error
	// All the full buffers we've created.
	buffers []io.Reader
	// The buffer we're currently writing to
	buf []byte
}

// Returns an io.Reader to all the bytes that have been written.
func (b *BufferingWriter) GetReader() io.Reader {
	if len(b.buf) > 0 {
		b.buffers = append(b.buffers, bytes.NewReader(b.buf))
	}
	return io.MultiReader(b.buffers...)
}

// Returns the amount of memory the buffer is using.
func (b *BufferingWriter) Cap() int {
	return chunkSize*len(b.buffers) + len(b.buf)
}

func (b *BufferingWriter) grow() {
	if len(b.buf) > 0 {
		b.buffers = append(b.buffers, bytes.NewReader(b.buf))
	}
	if err := b.Grow(chunkSize); err != nil {
		// We're done, use Limit zero value to turn things off.
		*b = BufferingWriter{}
	} else {
		b.buf = make([]byte, 0, chunkSize)
	}
}

// Write implements the io.Writer interface.
func (b *BufferingWriter) Write(p []byte) (n int, err error) {
	// When we cross limit just drop everything.
	if int64(b.Cap()) >= b.Limit {
		*b = BufferingWriter{}
		return len(p), nil
	}
	if cap(b.buf)-len(b.buf) == 0 {
		b.grow()
	}
	buf := b.buf
	towrite := p
	off := len(buf)
	room := cap(buf) - off
	if room < len(p) {
		towrite = p[:room]
		p = p[room:]
	} else {
		p = nil
	}
	n = copy(buf[off:off+room], towrite)
	if n != len(towrite) {
		panic(false)
	}
	b.buf = buf[:off+n]
	if len(p) > 0 {
		e, err := b.Write(p)
		if err != nil {
			return 0, err
		}
		n += e
	}
	return n, nil
}
