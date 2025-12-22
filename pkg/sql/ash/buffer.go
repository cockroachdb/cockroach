// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// RingBuffer is a thread-safe circular buffer for ASH samples.
// When the buffer is full, the oldest samples are overwritten.
type RingBuffer struct {
	mu struct {
		syncutil.Mutex
		samples []ASHSample
		// head is the index where the next sample will be written.
		head int
		// count is the number of valid samples in the buffer.
		count int
	}
	capacity int
}

// NewRingBuffer creates a new RingBuffer with the given capacity.
func NewRingBuffer(capacity int) *RingBuffer {
	if capacity <= 0 {
		capacity = 3600 // default: 1 hour at 1s interval
	}
	rb := &RingBuffer{
		capacity: capacity,
	}
	rb.mu.samples = make([]ASHSample, capacity)
	return rb
}

// Add adds a sample to the buffer. If the buffer is full, the oldest
// sample is overwritten.
func (rb *RingBuffer) Add(sample ASHSample) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.mu.samples[rb.mu.head] = sample
	rb.mu.head = (rb.mu.head + 1) % rb.capacity
	if rb.mu.count < rb.capacity {
		rb.mu.count++
	}
}

// GetAll returns a copy of all samples currently in the buffer,
// ordered from oldest to newest.
func (rb *RingBuffer) GetAll() []ASHSample {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.mu.count == 0 {
		return nil
	}

	result := make([]ASHSample, rb.mu.count)
	if rb.mu.count < rb.capacity {
		// Buffer is not full yet, samples start at index 0.
		copy(result, rb.mu.samples[:rb.mu.count])
	} else {
		// Buffer is full, oldest sample is at head.
		// Copy from head to end, then from start to head.
		firstPart := rb.capacity - rb.mu.head
		copy(result[:firstPart], rb.mu.samples[rb.mu.head:])
		copy(result[firstPart:], rb.mu.samples[:rb.mu.head])
	}
	return result
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

	if newCapacity == rb.capacity {
		return
	}

	// Get all current samples in order
	var oldSamples []ASHSample
	if rb.mu.count > 0 {
		oldSamples = make([]ASHSample, rb.mu.count)
		if rb.mu.count < rb.capacity {
			copy(oldSamples, rb.mu.samples[:rb.mu.count])
		} else {
			firstPart := rb.capacity - rb.mu.head
			copy(oldSamples[:firstPart], rb.mu.samples[rb.mu.head:])
			copy(oldSamples[firstPart:], rb.mu.samples[:rb.mu.head])
		}
	}

	// Create new buffer
	rb.mu.samples = make([]ASHSample, newCapacity)
	rb.capacity = newCapacity

	// Copy samples, keeping only the newest ones if necessary
	if len(oldSamples) > newCapacity {
		// Keep only the newest samples
		oldSamples = oldSamples[len(oldSamples)-newCapacity:]
	}

	copy(rb.mu.samples, oldSamples)
	rb.mu.count = len(oldSamples)
	rb.mu.head = rb.mu.count % newCapacity
}

