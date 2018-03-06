// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"context"

	"github.com/marusama/semaphore"

	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// concurrentRequestLimiter allows a configurable number of requests to run at
// once, while respecting context.Context cancellation and adding tracing with
// the configured name when a request has to block for the limiter.
type concurrentRequestLimiter struct {
	spanName string
	sem      semaphore.Semaphore
}

func makeConcurrentRequestLimiter(spanName string, limit int) concurrentRequestLimiter {
	return concurrentRequestLimiter{spanName: spanName, sem: semaphore.New(limit)}
}

func (l *concurrentRequestLimiter) beginLimitedRequest(ctx context.Context) error {
	if l.sem.TryAcquire(1) {
		return nil
	}
	// If not, start a span and begin waiting.
	ctx, span := tracing.ChildSpan(ctx, l.spanName)
	defer tracing.FinishSpan(span)
	return l.sem.Acquire(ctx, 1)
}

func (l *concurrentRequestLimiter) endLimitedRequest() {
	l.sem.Release(1)
}

func (l *concurrentRequestLimiter) resize(newLimit int) {
	l.sem.SetLimit(newLimit)
}
