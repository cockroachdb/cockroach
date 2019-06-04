// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package limit

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/marusama/semaphore"
)

// ConcurrentRequestLimiter wraps a simple semaphore, adding a tracing span when
// a request is forced to wait.
type ConcurrentRequestLimiter struct {
	spanName string
	sem      semaphore.Semaphore
}

// MakeConcurrentRequestLimiter creates a ConcurrentRequestLimiter.
func MakeConcurrentRequestLimiter(spanName string, limit int) ConcurrentRequestLimiter {
	return ConcurrentRequestLimiter{spanName: spanName, sem: semaphore.New(limit)}
}

// Begin attempts to reserve a spot in the pool, blocking if needed until the
// one is available or the context is canceled and adding a tracing span if it
// is forced to block.
func (l *ConcurrentRequestLimiter) Begin(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if l.sem.TryAcquire(1) {
		return nil
	}
	// If not, start a span and begin waiting.
	ctx, span := tracing.ChildSpan(ctx, l.spanName)
	defer tracing.FinishSpan(span)
	return l.sem.Acquire(ctx, 1)
}

// Finish indicates a concurrent request has completed and its reservation can
// be returned to the pool.
func (l *ConcurrentRequestLimiter) Finish() {
	l.sem.Release(1)
}

// SetLimit adjusts the size of the pool.
func (l *ConcurrentRequestLimiter) SetLimit(newLimit int) {
	l.sem.SetLimit(newLimit)
}
