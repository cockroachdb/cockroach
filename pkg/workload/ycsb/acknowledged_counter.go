// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package ycsb

import (
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
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
