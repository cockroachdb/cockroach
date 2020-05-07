// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ycsb

import (
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

const (
	// windowSize is the size of the window of pending acknowledgements.
	windowSize = 1 << 20
	// windowMask is the mask the apply to obtain an index in the window.
	windowMask = windowSize - 1
)

// AcknowledgedCounter keeps track of the largest value v such that all values
// in [initialCount, v) are acknowledged.
type AcknowledgedCounter struct {
	mu struct {
		syncutil.Mutex
		count  uint64
		window []bool
	}
}

// NewAcknowledgedCounter constructs a new AcknowledgedCounter with the given
// parameters.
func NewAcknowledgedCounter(initialCount uint64) *AcknowledgedCounter {
	c := &AcknowledgedCounter{}
	c.mu.count = initialCount
	c.mu.window = make([]bool, windowSize)
	return c
}

// Last returns the largest value v such that all values in [initialCount, v) are ackowledged.
func (c *AcknowledgedCounter) Last() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.count
}

// Acknowledge marks v as being acknowledged.
func (c *AcknowledgedCounter) Acknowledge(v uint64) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mu.window[v&windowMask] {
		return 0, errors.Errorf("Number of pending acknowledgements exceeded window size: %d has been acknowledged, but %d is not acknowledged", v, c.mu.count)
	}

	c.mu.window[v&windowMask] = true
	count := uint64(0)
	for c.mu.window[c.mu.count&windowMask] {
		c.mu.window[c.mu.count&windowMask] = false
		c.mu.count++
		count++
	}
	return count, nil
}
