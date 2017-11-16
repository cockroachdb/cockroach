// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License.

package sqlbase

import (
	"golang.org/x/net/context"
)

// Interval of Check() calls to wait between checks for context cancellation.
const cancelCheckInterval int64 = 1000

// CancelChecker is a helper object for repeatedly checking whether the associated context
// has been cancelled or not. Encapsulates all logic for waiting for cancelCheckInterval
// rows before actually checking for cancellation. The cancellation check
// has a significant time overhead, so it's not checked in every iteration.
type CancelChecker struct {
	// Reference to associated context to check.
	ctx context.Context

	// Number of times Check() has been called since last context cancellation check.
	callsSinceLastCheck int64

	// Last returned cancellation value.
	isCancelled bool
}

// NewCancelChecker returns a new CancelChecker.
func NewCancelChecker(ctx context.Context) *CancelChecker {
	return &CancelChecker{
		ctx: ctx,
	}
}

// Check returns an error if the associated query has been cancelled.
func (c *CancelChecker) Check() error {
	if !c.isCancelled && c.callsSinceLastCheck%cancelCheckInterval == 0 {
		select {
		case <-c.ctx.Done():
			c.isCancelled = true
		default:
			c.isCancelled = false
		}
	}

	c.callsSinceLastCheck++

	if c.isCancelled {
		return NewQueryCanceledError()
	}
	return nil
}
