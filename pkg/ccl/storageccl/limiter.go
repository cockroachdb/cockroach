// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/ccl/LICENSE

package storageccl

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// concurrentRequestLimiter allows a configurable number of requests to run at
// once, while respecting context.Context cancellation and adding tracing spans
// when a request has to block for the limiter.
type concurrentRequestLimiter struct {
	sem chan struct{}
}

func makeConcurrentRequestLimiter(limit int) concurrentRequestLimiter {
	return concurrentRequestLimiter{sem: make(chan struct{}, limit)}
}

func (l *concurrentRequestLimiter) beginLimitedRequest(ctx context.Context) error {
	// Check to see there's a slot immediately available.
	select {
	case l.sem <- struct{}{}:
		return nil
	default:
	}

	// If not, start a span and begin waiting.
	ctx, span := tracing.ChildSpan(ctx, "beginLimitedRequest")
	defer tracing.FinishSpan(span)

	// Wait until the context is done or we have a slot.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case l.sem <- struct{}{}:
		return nil
	}
}

func (l *concurrentRequestLimiter) endLimitedRequest() {
	<-l.sem
}
