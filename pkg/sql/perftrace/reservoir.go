// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package perftrace

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// ReservoirToken is returned by MaybeSample to track a sampling decision.
// It contains the index at which the span should be stored if sampled.
type ReservoirToken struct {
	shouldCapture bool
	index         int
}

// ShouldCapture returns whether this token indicates the span should be captured.
func (t ReservoirToken) ShouldCapture() bool {
	return t.shouldCapture
}

// Reservoir implements reservoir sampling for work spans.
// It maintains a fixed-size buffer of spans, using standard reservoir sampling
// to ensure a uniform sample of all spans seen.
type Reservoir struct {
	mu struct {
		syncutil.Mutex
		// spans is the reservoir buffer.
		spans []WorkSpan
		// count is the total number of spans seen (for reservoir sampling probability).
		count uint64
		// rng is the random number generator for sampling decisions.
		rng *rand.Rand
		// capacity is the maximum number of spans to store.
		capacity int
	}
}

// NewReservoir creates a new Reservoir with the given capacity.
func NewReservoir(capacity int) *Reservoir {
	r := &Reservoir{}
	r.mu.spans = make([]WorkSpan, 0, capacity)
	r.mu.rng = rand.New(rand.NewSource(rand.Int63()))
	r.mu.capacity = capacity
	return r
}

// MaybeSample decides at span start whether to capture this span.
// This allows the caller to avoid allocations for non-sampled spans.
// Returns a token that should be passed to Record() if ShouldCapture() is true.
func (r *Reservoir) MaybeSample() ReservoirToken {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.mu.count++
	count := r.mu.count
	capacity := r.mu.capacity

	// Standard reservoir sampling:
	// - First K items are always kept
	// - Subsequent items replace existing with probability K/n
	if len(r.mu.spans) < capacity {
		// Reservoir not full yet, always sample
		return ReservoirToken{shouldCapture: true, index: len(r.mu.spans)}
	}

	// Reservoir is full, sample with probability capacity/count
	if r.mu.rng.Float64() < float64(capacity)/float64(count) {
		// Replace a random existing span
		replaceIndex := r.mu.rng.Intn(capacity)
		return ReservoirToken{shouldCapture: true, index: replaceIndex}
	}

	return ReservoirToken{shouldCapture: false}
}

// Record adds a completed span to the reservoir.
// The token must have been obtained from MaybeSample() with ShouldCapture() == true.
func (r *Reservoir) Record(token ReservoirToken, span WorkSpan) {
	if !token.shouldCapture {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if token.index < len(r.mu.spans) {
		// Replace existing span
		r.mu.spans[token.index] = span
	} else if token.index == len(r.mu.spans) && len(r.mu.spans) < r.mu.capacity {
		// Append to reservoir (still filling up)
		r.mu.spans = append(r.mu.spans, span)
	}
	// Otherwise the index is stale (concurrent modification), skip
}

// Drain removes and returns all spans from the reservoir, resetting the count.
func (r *Reservoir) Drain() []WorkSpan {
	r.mu.Lock()
	defer r.mu.Unlock()

	spans := r.mu.spans
	r.mu.spans = make([]WorkSpan, 0, r.mu.capacity)
	r.mu.count = 0
	return spans
}

// Len returns the current number of spans in the reservoir.
func (r *Reservoir) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.mu.spans)
}

// Count returns the total number of spans seen since the last drain.
func (r *Reservoir) Count() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.count
}
