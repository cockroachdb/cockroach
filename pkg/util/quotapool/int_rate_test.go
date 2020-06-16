// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package quotapool_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRateLimiterBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	mt := quotapool.NewManualTime(t0)
	rl := quotapool.NewRateLimiter("test", 10, 20,
		quotapool.WithTimeSource(mt))
	ctx := context.Background()

	// Consume all of the burst over a number of requests.
	require.NoError(t, rl.WaitN(ctx, 1))
	require.NoError(t, rl.WaitN(ctx, 3))
	require.NoError(t, rl.WaitN(ctx, 4))
	require.NoError(t, rl.WaitN(ctx, 4))
	require.NoError(t, rl.WaitN(ctx, 8))

	// Set up some tools for the below scenarios.
	var (
		done   = make(chan struct{}, 1)
		doWait = func(n int64) {
			require.NoError(t, rl.WaitN(ctx, n))
			done <- struct{}{}
		}
		waitForTimers = func() {
			testutils.SucceedsSoon(t, func() error {
				if len(mt.Timers()) == 0 {
					return errors.Errorf("no timers found")
				}
				return nil
			})
		}
		ensureNotDone = func() {
			waitForTimers()
			select {
			case <-done:
				t.Fatalf("expected not yet done")
			case <-time.After(time.Microsecond):
			}
		}
	)
	{
		go doWait(5)
		ensureNotDone()
		mt.Advance(499 * time.Millisecond)
		ensureNotDone()
		mt.Advance(time.Millisecond)
		<-done
	}
	{
		go doWait(5)
		ensureNotDone()
		mt.Advance(499 * time.Millisecond)
		ensureNotDone()
		mt.Advance(500 * time.Microsecond)
		ensureNotDone()
		mt.Advance(1 * time.Millisecond)
		<-done
	}
	{
		// Fill the bucket all the way up.
		mt.Advance(2 * time.Second)
		go doWait(5)
		<-done
		go doWait(16)
		ensureNotDone()
		mt.Advance(99 * time.Millisecond)
		ensureNotDone()
		mt.Advance(1 * time.Millisecond)
		<-done
	}
	{
		// Fill the bucket all the way up.
		mt.Advance(2 * time.Second)
		acq3, err := rl.Acquire(ctx, 3)
		require.NoError(t, err)
		acq7, err := rl.Acquire(ctx, 7)
		go doWait(16)
		go doWait(14)
		mt.Advance(300 * time.Millisecond)
		ensureNotDone()
		acq3.Return()
		<-done
		acq7.Consume()
		ensureNotDone()
		mt.Advance(1399 * time.Millisecond)
		ensureNotDone()
		mt.Advance(time.Millisecond)
		<-done
	}
}
