// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ring

import (
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

// Buffer provides functionality akin to a []T, for cases that usually
// push to the back and pop from the front, and would like to reduce
// allocations. The assumption made here is that the capacity needed for
// holding the live entries is somewhat stable. The circular buffer grows as
// needed, and over time will shrink. Liveness of shrinking depends on new
// entries being pushed.
type Buffer[T any] struct {
	// len(buf) == cap(buf). These are powers of 2 and >= minCap.
	buf []T
	// first is in [0, len(buf)).
	first int
	// len is in [0, len(buf)].
	len int
	// pushesSinceCheck counts the number of push calls since the last time we
	// considered shrinking the buffer.
	pushesSinceCheck int64
	// maxObservedLen is the maximum observed len value since the last time we
	// considered shrinking the buffer.
	maxObservedLen int
}

// minCap is the smallest capacity and must be a power of 2. The choice of 32
// is arbitrary.
const minCap = 32

// 2^shrinkCheckInterval * cap is the threshold for checking whether the cap
// should be shrunk. This is used to amortize the cost of the check. The
// choice of 2^3 = 8 is arbitrary.
const shrinkCheckInterval = 3

// Push adds an entry to the end of the buffer.
func (cb *Buffer[T]) Push(a T) {
	needed := cb.len + 1
	cap := len(cb.buf)
	if needed > cap {
		// cap is a power of 2.
		cap = max(minCap, cap*2)
		// TODO(sumeer): panic if size is > math.MaxInt/2, since cb.first + cb.len
		// could overflow.
		cb.reallocate(cap)
	} else if cb.pushesSinceCheck > int64(cap)<<shrinkCheckInterval {
		// NB: don't bother with bit arithmetic since this conditional is rarely
		// evaluated. The constants here are arbitrary.
		//
		// This heuristic is subjective and arbitrary: for instance it doesn't
		// shrink if cap/2 is exactly what is needed, since that doesn't leave any
		// headroom. Also, the maxObservedLen is > needed-1 in this case, so that
		// part of the predicate may already disqualify from shrinking.
		if cap > minCap && cap > 3*cb.maxObservedLen && cap/2 > needed {
			// Shrink.
			cap = cap / 2
			cb.reallocate(cap)
		}
		cb.pushesSinceCheck = 0
		cb.maxObservedLen = 0
	}
	// NB: &(cap-1) is equivalent to %cap.
	cb.buf[(cb.first+cb.len)&(cap-1)] = a
	cb.len++
	cb.pushesSinceCheck++
	if cb.maxObservedLen < cb.len {
		cb.maxObservedLen = cb.len
	}
}

// Pop removes the first num entries.
//
// REQUIRES: num <= cb.len.
func (cb *Buffer[T]) Pop(num int) {
	if buildutil.CrdbTestBuild && num > cb.len {
		panic(errors.AssertionFailedf("num %d > cb.len %d", num, cb.len))
	}
	if num == 0 {
		// It is possible cb.cap is 0, so returning here avoids doing a possibly
		// incorrect <something> % cb.cap (though with two's complement the bit
		// operation below would happen to be correct).
		return
	}
	cb.len -= num
	// NB: &(len(cb.buf)-1) is equivalent to %len(cb.buf).
	cb.first = (cb.first + num) & (len(cb.buf) - 1)
}

// ShrinkToPrefix shrinks the buffer to retain the first num entries.
//
// REQUIRES: num <= cb.len.
func (cb *Buffer[T]) ShrinkToPrefix(num int) {
	if buildutil.CrdbTestBuild && num > cb.len {
		panic(errors.AssertionFailedf("num %d > cb.len %d", num, cb.len))
	}
	cb.len = num
}

// At returns the entry at index.
//
// REQUIRES: index < cb.len.
func (cb *Buffer[T]) At(index int) T {
	if buildutil.CrdbTestBuild && index >= cb.len {
		panic(errors.AssertionFailedf("index %d >= cb.len %d", index, cb.len))
	}
	// NB: &(len(cb.buf)-1) is equivalent to %len(cb.buf).
	return cb.buf[(cb.first+index)&(len(cb.buf)-1)]
}

// SetLast overwrites the last entry.
//
// REQUIRES: Length() > 0.
func (cb *Buffer[T]) SetLast(a T) {
	if buildutil.CrdbTestBuild && cb.len == 0 {
		panic(errors.AssertionFailedf("buffer is empty"))
	}
	// NB: &(len(cb.buf)-1) is equivalent to %len(cb.buf).
	cb.buf[(cb.first+cb.len-1)&(len(cb.buf)-1)] = a
}

// SetFirst overwrites the first entry.
//
// REQUIRES: Length() > 0.
func (cb *Buffer[T]) SetFirst(a T) {
	if buildutil.CrdbTestBuild && cb.len == 0 {
		panic(errors.AssertionFailedf("buffer is empty"))
	}
	cb.buf[cb.first] = a
}

// Length returns the current length.
func (cb *Buffer[T]) Length() int {
	return cb.len
}

func (cb *Buffer[T]) Clone() Buffer[T] {
	b := *cb
	b.buf = append([]T(nil), cb.buf...)
	return b
}

func (cb *Buffer[T]) reallocate(size int) {
	buf := make([]T, size)
	capacity := len(cb.buf)
	// cb.buf is split into a prefix and suffix, where the prefix is
	// cb.buf[first:first+n1] and the suffix is cb.buf[:n2].
	n1 := capacity - cb.first
	n2 := 0
	if n1 < cb.len {
		// Suffix is non-empty.
		n2 = cb.len - n1
	} else {
		// Suffix is empty.
		n1 = cb.len
	}
	if n1 > 0 {
		copy(buf, cb.buf[cb.first:cb.first+n1])
	}
	if n2 > 0 {
		copy(buf[n1:], cb.buf[:n2])
	}
	cb.first = 0
	cb.buf = buf
}
