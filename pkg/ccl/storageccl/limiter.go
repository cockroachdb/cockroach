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

const (
	// ParallelRequestsLimit is the number of Export or Import requests that can
	// run at once. Both of these requests generally do some read/write from the
	// network and cache results to a tmp file. In order to not exhaust the disk
	// or memory, or saturate the network, limit the number of these that can be
	// run in parallel. This number was chosen by a guess. If SST files are
	// likely to not be over 200MB, then 5 parallel workers hopefully won't use
	// more than 1GB of space in the tmp directory. It could be improved by more
	// measured heuristics.
	ParallelRequestsLimit = 5
)

var (
	parallelRequestsLimiter = make(chan struct{}, ParallelRequestsLimit)
)

func beginLimitedRequest(ctx context.Context) error {
	// Check to see there's a slot immediately available.
	select {
	case parallelRequestsLimiter <- struct{}{}:
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
	case parallelRequestsLimiter <- struct{}{}:
		return nil
	}
}

func endLimitedRequest() {
	<-parallelRequestsLimiter
}
