// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import "github.com/cockroachdb/cockroach/pkg/util/syncutil"

// RingBuffer is a thread-safe circular buffer for ASH samples.
// When the buffer is full, the oldest samples are overwritten.
//
// This is a custom implementation rather than using pkg/util/ring.Buffer
// because that type is a non-thread-safe growing deque, while ASH needs
// a thread-safe, fixed-capacity circular buffer with overwrite-on-full
// semantics. These are fundamentally different data structures.
type RingBuffer struct {
	mu struct {
		syncutil.Mutex
		samples []ASHSample
		// head is the index where the next sample will be written.
		head int
		// count is the number of valid samples in the buffer.
		count int
		// capacity is the maximum number of samples the buffer can hold.
		capacity int
	}
}

// NewRingBuffer creates a new RingBuffer with the given capacity.
func NewRingBuffer(capacity int) *RingBuffer {
	if capacity <= 0 {
		// arbitrary default capacity.
		capacity = 100_000
	}
	rb := &RingBuffer{}
	rb.mu.capacity = capacity
	rb.mu.samples = make([]ASHSample, capacity)
	return rb
}

// Add adds a sample to the buffer. If the buffer is full, the oldest
// sample is overwritten.
func (rb *RingBuffer) Add(sample ASHSample) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.mu.samples[rb.mu.head] = sample
	rb.mu.head = (rb.mu.head + 1) % rb.mu.capacity
	if rb.mu.count < rb.mu.capacity {
		rb.mu.count++
	}
}

// GetAll returns a copy of all samples currently in the buffer, ordered
// from oldest to newest. The caller may pass a pre-allocated result
// slice to avoid allocation; if nil or insufficient capacity, a new
// slice is allocated.
//
// The mutex is shared between Add (writer) and GetAll (reader). The
// sampler goroutine is the only writer, calling Add once per active
// goroutine per tick (default 1s). Even with thousands of active
// goroutines, each Add holds the lock for a single struct copy,
// so the total write-side hold time per tick is on the
// order of microseconds. GetAll holds the lock for a contiguous
// memcpy-like copy of the buffer, which is similarly fast. We do not
// expect meaningful contention between the two.
func (rb *RingBuffer) GetAll(result []ASHSample) []ASHSample {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.mu.count == 0 {
		if result == nil {
			return nil
		}
		return result[:0]
	}

	if cap(result) < rb.mu.count {
		result = make([]ASHSample, rb.mu.count)
	} else {
		result = result[:rb.mu.count]
	}
	if rb.mu.count < rb.mu.capacity {
		// Buffer is not full yet, samples start at index 0.
		copy(result, rb.mu.samples[:rb.mu.count])
	} else {
		// Buffer is full, oldest sample is at head.
		// Copy from head to end, then from start to head.
		firstPart := rb.mu.capacity - rb.mu.head
		copy(result[:firstPart], rb.mu.samples[rb.mu.head:])
		copy(result[firstPart:], rb.mu.samples[:rb.mu.head])
	}
	return result
}

// RangeReverse iterates all samples in the buffer from newest to
// oldest, calling fn for each sample. If fn returns false, iteration
// stops early. The lock is held for the duration of the iteration
// so fn must not call other RingBuffer methods.
//
// Newest-to-oldest order allows callers to stop early once they
// reach samples older than a cutoff time, avoiding a full scan of
// the buffer.
func (rb *RingBuffer) RangeReverse(fn func(ASHSample) bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.mu.count == 0 {
		return
	}

	// The newest sample is at (head - 1). Walk backwards from there.
	for i := range rb.mu.count {
		idx := (rb.mu.head - 1 - i + rb.mu.capacity) % rb.mu.capacity
		if !fn(rb.mu.samples[idx]) {
			return
		}
	}
}

// Len returns the number of samples currently in the buffer.
func (rb *RingBuffer) Len() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.mu.count
}

// Clear removes all samples from the buffer.
func (rb *RingBuffer) Clear() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.mu.head = 0
	rb.mu.count = 0
}

// Resize changes the capacity of the buffer. If the new capacity is smaller
// than the current number of samples, the oldest samples are discarded.
func (rb *RingBuffer) Resize(newCapacity int) {
	if newCapacity <= 0 {
		return
	}

	rb.mu.Lock()
	defer rb.mu.Unlock()

	if newCapacity == rb.mu.capacity {
		return
	}

	// Get all current samples in order.
	var oldSamples []ASHSample
	if rb.mu.count > 0 {
		oldSamples = make([]ASHSample, rb.mu.count)
		if rb.mu.count < rb.mu.capacity {
			copy(oldSamples, rb.mu.samples[:rb.mu.count])
		} else {
			firstPart := rb.mu.capacity - rb.mu.head
			copy(oldSamples[:firstPart], rb.mu.samples[rb.mu.head:])
			copy(oldSamples[firstPart:], rb.mu.samples[:rb.mu.head])
		}
	}

	// Create new buffer.
	rb.mu.samples = make([]ASHSample, newCapacity)
	rb.mu.capacity = newCapacity

	// Copy samples, keeping only the newest ones if necessary.
	if len(oldSamples) > newCapacity {
		// Keep only the newest samples.
		oldSamples = oldSamples[len(oldSamples)-newCapacity:]
	}

	copy(rb.mu.samples, oldSamples)
	rb.mu.count = len(oldSamples)
	rb.mu.head = rb.mu.count % newCapacity
}
