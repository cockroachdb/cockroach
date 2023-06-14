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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
)

// ConcurrentRequestLimiter wraps a simple semaphore, adding a tracing span when
// a request is forced to wait.
type ConcurrentRequestLimiter struct {
	spanName string
	sem      *quotapool.IntPool
	disabled bool
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
		return nil, errors.Wrap(err, "limiter begin")
	}
	if l.disabled {
		return noopReservation{}, nil // nothing to do
	}

	res, err := l.sem.TryAcquire(ctx, 1)
	if errors.Is(err, quotapool.ErrNotEnoughQuota) {
		var span *tracing.Span
		ctx, span = tracing.ChildSpan(ctx, l.spanName)
		defer span.Finish()
		span.RecordStructured(&types.StringValue{Value: fmt.Sprintf("%d requests are waiting", l.sem.Len())})
		res, err = l.sem.Acquire(ctx, 1)
	}
	return res, err
}

// SetLimit adjusts the size of the pool. If limit == 0, the concurrency limiter
// is disabled.
func (l *ConcurrentRequestLimiter) SetLimit(newLimit int) {
	l.disabled = newLimit == 0
	l.sem.UpdateCapacity(uint64(newLimit))
}

type noopReservation struct{}

var _ Reservation = noopReservation{}

// Release implements the Reservation interface.
func (n noopReservation) Release() {}
