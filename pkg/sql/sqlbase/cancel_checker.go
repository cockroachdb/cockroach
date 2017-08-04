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
	"errors"

	"golang.org/x/net/context"
)

// Interval of rows to wait between cancellation checks.
const cancelCheckInterval int64 = 1000

// CancelChecker is an interface for an abstract cancellation checker,
// implemented by contextCancelChecker and nullCancelChecker.
type CancelChecker interface {
	Check() error
}

// Cancellation checker for internal planners (does nothing).
type nullCancelChecker struct{}

func (c *nullCancelChecker) Check() error {
	return nil
}

// Helper object for repeatedly checking whether the associated context has been
// cancelled or not. Encapsulates all logic for waiting for cancelCheckInterval
// rows before actually checking for cancellation. The cancellation check
// has a significant time overhead, so it's not checked in every iteration.
type contextCancelChecker struct {
	// Reference to associated context to check.
	ctx context.Context

	// Rows elapsed since the last time the cancellation check was made.
	rowsSinceLastCheck int64

	// Last returned cancellation value.
	isCancelled bool
}

// MakeCancelChecker returns a new CancelChecker.
func MakeCancelChecker(ctx context.Context) CancelChecker {
	if ctx == nil {
		return &nullCancelChecker{}
	}

	return &contextCancelChecker{
		ctx: ctx,
	}
}

// Check returns an error if the associated query has been cancelled.
func (c *contextCancelChecker) Check() error {
	if !c.isCancelled && c.rowsSinceLastCheck%cancelCheckInterval == 0 {
		select {
		case <-c.ctx.Done():
			c.isCancelled = true
		default:
			c.isCancelled = false
		}
	}

	c.rowsSinceLastCheck++

	if c.isCancelled {
		return errors.New("query execution cancelled")
	}
	return nil
}

var _ CancelChecker = &contextCancelChecker{}
var _ CancelChecker = &nullCancelChecker{}
