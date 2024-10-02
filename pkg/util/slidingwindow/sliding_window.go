// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package slidingwindow

import "time"

// Swag represents a sliding window aggregator over a binary operation.
// The aggregator will aggregate recorded values in two ways:
// (1) Within each window, the binary operation is applied when recording a new
//
//	value into the current window.
//
// (2) On Query, the binary operation is accumulated over every window, from
//
//	most to least recent.
//
// The binary operator function must therefore be:
//
//	associative :: binOp(binOp(a,b), c) = binOp(a,binOp(b,c))
//
// In order to have correct results. Note that this does not allow for a more
// general class of aggregators that may be  associative, such as geometric
// mean, bloom filters etc. These require special treatment with user defined
// functions for lift(e), lower(a) and combine(v1,v2)
// (https://dl.acm.org/doi/pdf/10.1145/3093742.3093925).
//
// query: O(k), append: O(k), space O(k), k = |windows|.
// The average case append is O(1), when no windows
// require rotating; the size requirement is 32 + 8k bytes.
type Swag struct {
	curIdx         int
	windows        []*float64
	lastRotate     time.Time
	rotateInterval time.Duration
	binOp          func(acc, val float64) float64
}

// NewSwag returns a new sliding window aggregator.
func NewSwag(
	now time.Time, interval time.Duration, size int, binOp func(acc, val float64) float64,
) *Swag {
	windows := make([]*float64, size)
	var first float64
	windows[0] = &first
	return &Swag{
		curIdx:         0,
		windows:        windows,
		lastRotate:     now,
		rotateInterval: interval,
		binOp:          binOp,
	}
}

// Record takes a value and applies the binary operation with the current
// bucket and the value.
func (s *Swag) Record(now time.Time, val float64) {
	s.maybeRotate(now)
	*s.windows[s.curIdx] = s.binOp(*s.windows[s.curIdx], val)
}

// maybeRotate checks the passed in time with the last rotate time. If the
// duration elapsed is greater than the rotate interval, it will rotate the
// windows, adding the interval to the last rotate time. This continues until
// the duration elapsed no longer greater last rotate +  interval.
func (s *Swag) maybeRotate(now time.Time) {
	sinceLastRotate := now.Sub(s.lastRotate)
	if sinceLastRotate < s.rotateInterval {
		return
	}

	size := len(s.windows)
	shift := int(sinceLastRotate / s.rotateInterval)
	for i := 0; i < shift; i++ {
		s.curIdx = (s.curIdx + 1) % size
		s.lastRotate = s.lastRotate.Add(s.rotateInterval)
		var next float64
		s.windows[s.curIdx] = &next
	}
}

// Query applies the binOp across each window accumulated, from most recent to
// least recent window. This requires that the binOp fn is associative.
func (s *Swag) Query(now time.Time) (float64, time.Duration) {
	windows := s.Windows(now)
	timeSinceRotate := now.Sub(s.lastRotate)

	var accumulator float64
	var duration time.Duration
	for i, next := range windows {
		accumulator = s.binOp(accumulator, next)
		if i == 0 {
			duration += time.Duration(float64(timeSinceRotate))
		} else {
			duration += time.Duration(float64(s.rotateInterval))
		}
	}
	return accumulator, duration
}

// Windows returns the currently populated windows, in most recent to least
// recent order. It will not return unpopulated windows, if total duration is
// less than size * rotateInterval.
func (s *Swag) Windows(now time.Time) []float64 {
	s.maybeRotate(now)
	size := len(s.windows)
	ret := make([]float64, 0, 1)

	for i := 0; i < size; i++ {
		next := s.windows[(s.curIdx+size-i)%size]
		if next == nil {
			break
		}
		ret = append(ret, *next)
	}
	return ret
}
