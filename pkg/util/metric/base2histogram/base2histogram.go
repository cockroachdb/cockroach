// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package base2histogram is a Go port of the Rust base2histogram library
// (github.com/drmingdrmer/base2histogram). It provides an exponential histogram
// with pure-integer bucket mapping, fixed memory footprint, and slope-clamped
// trapezoidal quantile estimation.
//
// The bucket layout uses a configurable WIDTH parameter (default 3) that
// controls granularity. Each power-of-2 octave is divided into 2^(WIDTH-1)
// linearly-spaced sub-buckets. Values below 2^WIDTH get exact 1:1 mapping.
// The bucket index is computed using only bits.Len64, shifts, and masks — no
// floating-point arithmetic.
//
// At WIDTH=3, the histogram has 252 buckets covering [0, 2^63), using ~2KB of
// memory per instance. Sub-buckets within each octave are linearly spaced,
// which means relative error varies ~2x within each octave (worst at the
// bottom, best at the top).
package base2histogram

import (
	"math/bits"
	"sync/atomic"
)

const (
	// Width controls the number of sub-buckets per power-of-2 octave.
	// Each octave has 2^(Width-1) sub-buckets.
	Width = 3

	// GroupSize is the number of sub-buckets per group (power-of-2 octave).
	GroupSize = 1 << (Width - 1) // 4

	// Mask is used to extract the sub-bucket offset within a group.
	Mask = GroupSize - 1 // 3

	// NumGroups is the number of groups beyond the exact-mapping range.
	// For 64-bit values: groups cover MSB positions from Width to 63.
	NumGroups = 64 - Width + 1 // 62

	// NumBuckets is the total number of buckets.
	// GroupSize exact buckets + NumGroups * GroupSize group buckets.
	NumBuckets = GroupSize + NumGroups*GroupSize // 4 + 62*4 = 252

	// RegionSize is the number of buckets per region sum entry.
	RegionSize = 16

	// NumRegions is the number of region sum entries.
	NumRegions = (NumBuckets + RegionSize - 1) / RegionSize // 16
)

// BucketIndex computes the bucket index for a uint64 value. This is the hot
// path — pure integer arithmetic with no floating point.
//
// For values < GroupSize, the index is the value itself (exact mapping).
// For larger values, the index is computed from the position of the highest
// set bit (the "group") and the next (Width-1) bits (the "offset" within
// the group).
func BucketIndex(v uint64) int {
	if v < GroupSize {
		return int(v)
	}
	msb := bits.Len64(v) // 1-indexed position of highest set bit
	group := msb - Width
	offset := int((v >> group) & Mask)
	return GroupSize + group*GroupSize + offset
}

// BucketMinValue returns the minimum value that maps to the given bucket index.
func BucketMinValue(idx int) uint64 {
	if idx < GroupSize {
		return uint64(idx)
	}
	group := (idx - GroupSize) / GroupSize
	offset := (idx - GroupSize) % GroupSize
	// The minimum value for this bucket is: (GroupSize + offset) << group
	return uint64(GroupSize+offset) << group
}

// BucketMaxValue returns the maximum value (inclusive) that maps to the given
// bucket index.
func BucketMaxValue(idx int) uint64 {
	if idx >= NumBuckets-1 {
		return 1<<63 - 1
	}
	next := BucketMinValue(idx + 1)
	if next == 0 {
		// Overflow — the next bucket's min wrapped around.
		return 1<<63 - 1
	}
	return next - 1
}

// BucketWidth returns the width (number of distinct values) of the given bucket.
func BucketWidth(idx int) uint64 {
	return BucketMaxValue(idx) - BucketMinValue(idx) + 1
}

// BucketMidpoint returns the midpoint value of the given bucket.
func BucketMidpoint(idx int) float64 {
	lo := BucketMinValue(idx)
	hi := BucketMaxValue(idx)
	return float64(lo)/2.0 + float64(hi)/2.0
}

// Counters is a single histogram instance with lock-free atomic counters.
// It maintains both per-bucket counts and region sums (sums of every
// RegionSize consecutive buckets) for accelerated rank lookup.
type Counters struct {
	counts     [NumBuckets]atomic.Uint64
	regionSums [NumRegions]atomic.Uint64
	sum        atomic.Int64
	count      atomic.Uint64
}

// NewCounters allocates a new Counters instance.
func NewCounters() *Counters {
	return &Counters{}
}

// Record adds an int64 value to the histogram. Negative values are recorded
// in bucket 0.
func (c *Counters) Record(v int64) {
	c.count.Add(1)
	c.sum.Add(v)
	var uv uint64
	if v > 0 {
		uv = uint64(v)
	}
	idx := BucketIndex(uv)
	c.counts[idx].Add(1)
	c.regionSums[idx/RegionSize].Add(1)
}

// Snapshot returns a point-in-time copy of the counters.
func (c *Counters) Snapshot() Snapshot {
	s := Snapshot{
		TotalCount: c.count.Load(),
		TotalSum:   c.sum.Load(),
	}
	for i := range s.Counts {
		s.Counts[i] = c.counts[i].Load()
	}
	for i := range s.RegionSums {
		s.RegionSums[i] = c.regionSums[i].Load()
	}
	return s
}

// Snapshot is a point-in-time, non-atomic copy of a Counters instance,
// suitable for quantile computation.
type Snapshot struct {
	Counts     [NumBuckets]uint64
	RegionSums [NumRegions]uint64
	TotalCount uint64
	TotalSum   int64
}

// Histogram is the top-level histogram type.
type Histogram struct {
	counters *Counters
}

// New creates a new Histogram.
func New() *Histogram {
	return &Histogram{
		counters: NewCounters(),
	}
}

// Record adds an int64 value to the histogram.
func (h *Histogram) Record(v int64) {
	h.counters.Record(v)
}

// Snapshot returns a point-in-time snapshot of the histogram.
func (h *Histogram) Snapshot() Snapshot {
	return h.counters.Snapshot()
}
