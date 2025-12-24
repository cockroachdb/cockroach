// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package allstacks

import (
	"math/bits"
	"runtime"

	"github.com/cockroachdb/cockroach/pkg/util/debugutil"
)

// Get returns all stacks, except if that takes more than 512mb of memory, in
// which case it returns only 512mb worth of stacks (clipping the last stack
// if necessary).
func Get() debugutil.SafeStack {
	return GetWithBuf(nil)
}

// GetWithBuf is like Get, but tries to use the provided slice first, allocating
// a new, larger, slice only if necessary.
func GetWithBuf(buf []byte) debugutil.SafeStack {
	buf = buf[:cap(buf)]

	const minSize = 1 << 20 // 1MB
	const maxSize = 1 << 29 // 512MB

	// We don't know how big the traces are, so grow a few times if they don't
	// fit. Start large, though.
	initialSize := min(max(minSize, initialBufferSize(runtime.NumGoroutine())), maxSize)

	for n := initialSize; n <= maxSize; n *= 2 {
		if len(buf) < n {
			buf = make([]byte, n)
		}
		nbytes := runtime.Stack(buf, true /* all */)
		if nbytes < len(buf) {
			return buf[:nbytes]
		}
	}
	return buf // returning full 512MB slice
}

// initialBufferSize computes the initial buffer size for capturing stack traces
// based on the number of goroutines. It returns a power-of-2 size without any
// min/max clamping.
func initialBufferSize(numGoroutines int) int {
	// ~6k bytes characters is representative of a relatively deep stack (~50
	// frames) and was determined by randomly picking a long one out of a
	// realistic debug.zip.
	const bytesPerGoroGuess = 6000
	guessedBytes := uint(numGoroutines * bytesPerGoroGuess)

	// Compute the ceiling power of 2 for guessedBytes.
	if guessedBytes <= 1 {
		return 1
	}
	return 1 << bits.Len(guessedBytes-1)
}
