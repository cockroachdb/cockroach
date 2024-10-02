// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var useFastRetry = envutil.EnvOrDefaultBool(
	"COCKROACH_CHANGEFEED_TESTING_FAST_RETRY", false)

// getRetry returns retry object for changefeed.
func getRetry(ctx context.Context) Retry {
	opts := retry.Options{
		InitialBackoff: 5 * time.Second,
		Multiplier:     2,
		MaxBackoff:     10 * time.Minute,
	}

	if useFastRetry {
		opts = retry.Options{
			InitialBackoff: 5 * time.Millisecond,
			Multiplier:     2,
			MaxBackoff:     250 * time.Millisecond,
		}
	}

	return Retry{Retry: retry.StartWithCtx(ctx, opts)}
}

func testingUseFastRetry() func() {
	useFastRetry = true
	return func() {
		useFastRetry = false
	}
}

// reset retry state after changefeed ran for that much time
// without errors.
const resetRetryAfter = 10 * time.Minute

// Retry is a thin wrapper around retry.Retry which
// resets retry state if changefeed been running for sufficiently
// long time.
type Retry struct {
	retry.Retry
	lastRetry time.Time
}

// Next returns whether the retry loop should continue, and blocks for the
// appropriate length of time before yielding back to the caller.
// If the last call to Next() happened long time ago, the amount of time
// to wait gets reset.
func (r *Retry) Next() bool {
	defer func() {
		r.lastRetry = timeutil.Now()
	}()
	if timeutil.Since(r.lastRetry) > resetRetryAfter {
		r.Reset()
	}
	return r.Retry.Next()
}
