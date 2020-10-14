// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cancelchecker

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// CancelChecker is a helper object for repeatedly checking whether the associated context
// has been canceled or not. Encapsulates all logic for waiting for cancelCheckInterval
// rows before actually checking for cancellation. The cancellation check
// has a significant time overhead, so it's not checked in every iteration.
type CancelChecker struct {
	// Reference to associated context to check.
	ctx context.Context

	// Number of times Check() has been called since last context cancellation check.
	callsSinceLastCheck uint32
}

// NewCancelChecker returns a new CancelChecker.
// TODO(yuzefovich): audit all processors to make sure that the ones that
// should use the cancel checker actually do so.
func NewCancelChecker(ctx context.Context) *CancelChecker {
	return &CancelChecker{
		ctx: ctx,
	}
}

// Check returns an error if the associated query has been canceled.
func (c *CancelChecker) Check() error {
	// Interval of Check() calls to wait between checks for context
	// cancellation. The value is a power of 2 to allow the compiler to use
	// bitwise AND instead of division.
	const cancelCheckInterval = 1024

	if atomic.LoadUint32(&c.callsSinceLastCheck)%cancelCheckInterval == 0 {
		select {
		case <-c.ctx.Done():
			// Once the context is canceled, we no longer increment
			// callsSinceLastCheck and will fall into this path on subsequent calls
			// to Check().
			return QueryCanceledError
		default:
		}
	}

	// Increment. This may rollover when the 32-bit capacity is reached,
	// but that's all right.
	atomic.AddUint32(&c.callsSinceLastCheck, 1)
	return nil
}

// Reset resets this cancel checker with a fresh context.
func (c *CancelChecker) Reset(ctx context.Context) {
	*c = CancelChecker{
		ctx: ctx,
	}
}

// QueryCanceledError is an error representing query cancellation.
var QueryCanceledError = pgerror.New(
	pgcode.QueryCanceled, "query execution canceled")
