// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This is a fork of pkg/json package.

// Copyright (c) 2020, Dave Cheney <dave@cheney.net>
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//   - Redistributions of source code must retain the above copyright notice, this
//     list of conditions and the following disclaimer.
//
//   - Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package tokenizer

import "io"

// A byteReader implements a sliding window over an io.Reader.
type byteReader struct {
	data   []byte
	offset int
	r      io.Reader
	err    error
}

// release discards n bytes from the front of the window.
func (b *byteReader) release(n int) {
	b.offset += n
}

// window returns the current window.
// The window is invalidated by calls to release or extend.
func (b *byteReader) window(offset int) []byte {
	return b.data[b.offset+offset:]
}

// tuning constants for byteReader.extend.
const (
	newBufferSize = 4096
	minReadSize   = newBufferSize >> 2
)

// extend extends the window with data from the underlying reader.
func (b *byteReader) extend() int {
	if b.err != nil {
		return 0
	}

	remaining := len(b.data) - b.offset
	if remaining == 0 {
		b.data = b.data[:0]
		b.offset = 0
	}
	if cap(b.data)-len(b.data) >= minReadSize {
		// nothing to do, enough space exists between len and cap.
	} else if cap(b.data)-remaining >= minReadSize {
		// buffer has enough space if we move the data to the front.
		b.compact()
	} else {
		// otherwise, we must allocate/extend a new buffer
		b.grow()
	}
	remaining += b.offset
	n, err := b.r.Read(b.data[remaining:cap(b.data)])
	// reduce length to the existing plus the data we read.
	b.data = b.data[:remaining+n]
	b.err = err
	return n
}

// grow grows the buffer, moving the active data to the front.
func (b *byteReader) grow() {
	buf := make([]byte, max(cap(b.data)*2, newBufferSize))
	copy(buf, b.data[b.offset:])
	b.data = buf
	b.offset = 0
}

// compact moves the active data to the front of the buffer.
func (b *byteReader) compact() {
	copy(b.data, b.data[b.offset:])
	b.offset = 0
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
