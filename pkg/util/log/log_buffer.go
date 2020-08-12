// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import "bytes"

// buffer holds a byte Buffer for reuse while constructing log lines
// prior to sending them to a sync buffer or log stream.
// The zero value of this struct is ready for use.
// Allocations are optimized by using a free list.
type buffer struct {
	bytes.Buffer
	tmp [64]byte // temporary byte array for creating headers.
}

// newBuffer is the constructor for the sync.Pool.
func newBuffer() interface{} { return new(buffer) }

// getBuffer returns a new, ready-to-use buffer.
func getBuffer() *buffer {
	b := logging.bufPool.Get().(*buffer)
	b.Reset()
	return b
}

// putBuffer returns a buffer to the free list.
func putBuffer(b *buffer) {
	if b.Len() >= 256 {
		// Let big buffers die a natural death.
		return
	}
	logging.bufPool.Put(b)
}

// Some custom tiny helper functions to print the log header efficiently.

const digits = "0123456789"

// twoDigits formats a zero-prefixed two-digit integer at buf.tmp[i].
// Returns two.
func (buf *buffer) twoDigits(i, d int) int {
	buf.tmp[i+1] = digits[d%10]
	d /= 10
	buf.tmp[i] = digits[d%10]
	return 2
}

// nDigits formats an n-digit integer at buf.tmp[i],
// padding with pad on the left.
// It assumes d >= 0. Returns n.
func (buf *buffer) nDigits(n, i, d int, pad byte) int {
	j := n - 1
	for ; j >= 0 && d > 0; j-- {
		buf.tmp[i+j] = digits[d%10]
		d /= 10
	}
	for ; j >= 0; j-- {
		buf.tmp[i+j] = pad
	}
	return n
}

// someDigits formats a variable-width integer at buf.tmp[i].
func (buf *buffer) someDigits(i, d int) int {
	// Print into the top, then copy down. We know there's space for at least
	// a 10-digit number.
	j := len(buf.tmp)
	for {
		j--
		buf.tmp[j] = digits[d%10]
		d /= 10
		if d == 0 {
			break
		}
	}
	return copy(buf.tmp[i:], buf.tmp[j:])
}
