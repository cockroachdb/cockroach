// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package limit

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// ConcurrentRequestLimiter wraps a simple semaphore, adding a tracing span when
// a request is forced to wait.
type ConcurrentRequestLimiter struct {
	spanName string
	sem      *quotapool.IntPool
}

// Reservation is an allocation from a limiter which should be released once the
// limited task has been completed.
type Reservation interface {
	Release()
}

// MakeConcurrentRequestLimiter creates a ConcurrentRequestLimiter.
func MakeConcurrentRequestLimiter(spanName string, limit int) ConcurrentRequestLimiter {
	return ConcurrentRequestLimiter{
		spanName: spanName,
		sem:      quotapool.NewIntPool(spanName, uint64(limit)),
	}
}

// Begin attempts to reserve a spot in the pool, blocking if needed until the
// one is available or the context is canceled and adding a tracing span if it
// is forced to block.
func (l *ConcurrentRequestLimiter) Begin(ctx context.Context) (Reservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	res, err := l.sem.TryAcquire(ctx, 1)
	if errors.Is(err, quotapool.ErrNotEnoughQuota) {
		var span *tracing.Span
		ctx, span = tracing.ChildSpan(ctx, l.spanName)
		defer span.Finish()
		res, err = l.sem.Acquire(ctx, 1)
	}
	return res, err
}

// SetLimit adjusts the size of the pool.
func (l *ConcurrentRequestLimiter) SetLimit(newLimit int) {
	l.sem.UpdateCapacity(uint64(newLimit))
}
