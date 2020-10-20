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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRateLimiterBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	mt := timeutil.NewManualTime(t0)
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
		waitForTimersNotEqual = func(old time.Time) (first time.Time) {
			testutils.SucceedsSoon(t, func() error {
				timers := mt.Timers()
				if len(timers) == 0 {
					return errors.Errorf("no timers found")
				}
				if timers[0].Equal(old) {
					return errors.Errorf("timer is still %v", old)
				}
				first = timers[0]
				return nil
			})
			return first
		}
		waitForTimers = func() time.Time {
			return waitForTimersNotEqual(time.Time{})
		}
		ensureNotDone = func() time.Time {
			timer := waitForTimers()
			select {
			case <-done:
				t.Fatalf("expected not yet done")
			case <-time.After(time.Microsecond):
			}
			return timer
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
		require.NoError(t, err)
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
	{
		// Fill the bucket all the way up.
		mt.Advance(2 * time.Second)

		// Consume a small amount.
		go doWait(2)
		<-done

		// Attempt to consume more than the burst and observe blocking waiting for
		// the bucket to be full.
		go doWait(30)
		ensureNotDone()
		mt.Advance(199 * time.Millisecond)
		ensureNotDone()
		mt.Advance(time.Millisecond)
		<-done

		// Advance time and demonstrate the acquisition put the limiter into debt.
		// The limiter should be in debt by 10 so it will take 2s for there to be
		// enough quota.
		go doWait(10)
		ensureNotDone()
		mt.Advance(1999 * time.Millisecond)
		ensureNotDone()
		mt.Advance(2 * time.Millisecond)
		<-done
	}
	{
		// Fill the bucket all the way up.
		mt.Advance(2 * time.Second)

		// Consume some. There should be 10 in the bucket.
		go doWait(10)
		<-done

		// THis should need to wait one second for the bucket to fill up.
		go doWait(30)
		ensureNotDone()

		// Adjust the rate and the burst down. This should move the current
		// capacity down to 0 and lower the burst. It will now take 10 seconds
		// before the bucket is full.
		rl.UpdateLimit(1, 10)
		mt.Advance(9 * time.Second)
		ensureNotDone()
		mt.Advance(time.Second)
		<-done

		// At this point, the limiter should be 20 in debt so it should take
		// 20s before the current goroutine is unblocked.
		go doWait(2)
		prevTimer := ensureNotDone()

		// Adjust the rate and burst up. The burst delta is 10, so the debt should
		// reduce to 10 and the rate is doubled. In 6 seconds the goroutine should
		// unblock.
		rl.UpdateLimit(2, 20)
		waitForTimersNotEqual(prevTimer)
		mt.Advance(5 * time.Second)
		ensureNotDone()
		mt.Advance(time.Second)
		<-done

		// Set the limit and burst back to the default values.
		rl.UpdateLimit(10, 20)
	}
	{
		// Fill the bucket all the way up.
		mt.Advance(2 * time.Second)

		// Consume some. There should be 10 in the bucket.
		go doWait(10)
		<-done

		require.False(t, rl.AdmitN(11))
		require.True(t, rl.AdmitN(9)) // 1 left
		require.False(t, rl.AdmitN(2))
		require.True(t, rl.AdmitN(1)) // 0 left
	}
}

// TestRateLimitWithVerySmallDelta ensures that in cases where the delta is
// very small relative to the rate that nothing bad happens. In particular the
// concern arises when the delta/rate is less than nanosecond/second.
func TestRateLimiterWithVerySmallDelta(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	mt := timeutil.NewManualTime(t0)
	rl := quotapool.NewRateLimiter("test", 2e9, 1e10,
		quotapool.WithTimeSource(mt))
	ctx := context.Background()
	require.NoError(t, rl.WaitN(ctx, 1))
	errCh := make(chan error)
	// Attempt to acquire the entire quota, we should be 1 short.
	// That means we need to acquire 1 and the rate is 2e9 so it'll happen in
	// half a nanosecond.
	go func() { errCh <- rl.WaitN(ctx, 1e10) }()

	// Ensure that we indeed block on a timer.
	testutils.SucceedsSoon(t, func() error {
		if len(mt.Timers()) == 0 {
			return errors.Errorf("no timers found")
		}
		return nil
	})

	// Advance the clock by the nanosecond and ensure that we get notified.
	mt.Advance(time.Nanosecond)
	require.NoError(t, <-errCh)
}

// TestRateLimiterMinimumWait tests that the WithMinimumWait option works.
func TestRateLimiterMinimumWait(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	mt := timeutil.NewManualTime(t0)
	rl := quotapool.NewRateLimiter("test", 2e9, 1e10,
		quotapool.WithTimeSource(mt), quotapool.WithMinimumWait(time.Microsecond))
	ctx := context.Background()
	require.NoError(t, rl.WaitN(ctx, 1))
	errCh := make(chan error)
	// Attempt to acquire the entire quota, we should be 1 short.
	// That means we need to acquire 1 and the rate is 2e9 so it'll happen in
	// half a nanosecond. We want to see that we block for the minimum duration,
	// 1us.
	go func() { errCh <- rl.WaitN(ctx, 1e10) }()

	// Ensure that we indeed block on a timer.
	testutils.SucceedsSoon(t, func() error {
		if len(mt.Timers()) == 0 {
			return errors.Errorf("no timers found")
		}
		return nil
	})
	require.EqualValues(t, t0.Add(time.Microsecond), mt.Timers()[0])
	// Advance the clock by the microsecond and ensure that we get notified.
	mt.Advance(time.Microsecond)
	require.NoError(t, <-errCh)
}
