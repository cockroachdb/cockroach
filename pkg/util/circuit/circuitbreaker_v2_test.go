// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package circuit

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func testLogBridge(t *testing.T) EventLogBridge {
	return EventLogBridge{Logf: func(format redact.SafeString, args ...interface{}) {
		t.Logf(string(format), args...)
	}}
}

func TestBreakerV2(t *testing.T) {
	var report func(err error)
	var done func()
	b := NewBreakerV2(OptionsV2{
		ShouldTrip: func(err error) error { return err },
		AsyncProbe: func(_report func(error), _done func()) {
			if report != nil || done != nil {
				t.Error("probe launched twice")
			}
			report = _report
			done = _done
		},
		EventHandler: testLogBridge(t),
	})
	require.NoError(t, b.Err())
	require.Nil(t, report)
	require.Nil(t, done)
	b.Report(errors.New("boom"))
	require.NotNil(t, report)
	require.NotNil(t, done)
	{
		err := b.Err()
		require.True(t, errors.Is(err, ErrBreakerOpen()), "%+v", err)
		// Feeding the output error into the breaker again should not create
		// longer error chains (check pointer equality to verify that nothing
		// changed).
		b.Report(err)
		require.Equal(t, err, b.Err())
		b.Report(errors.Wrap(b.Err(), "more stuff"))
		require.Equal(t, err, b.Err())
	}
	b.Reset()
	require.NoError(t, b.Err())

	{
		// The probe reports an error. That error should update that returned by the
		// breaker.
		refErr := errors.New("probe error")
		report(refErr)
		err := b.Err()
		require.True(t, errors.Is(err, refErr), "%+v not marked as %+v", err, refErr)
	}

	{
		// An error is passed to `b.Report`. This is somewhat unusual, after all
		// the point of the breaker is to stop people from trying, but if it
		// happens we update the breaker's error just the same.
		refErr := errors.New("client error")
		b.Report(refErr)
		err := b.Err()
		require.True(t, errors.Is(err, refErr), "%+v not marked as %+v", err, refErr)
	}

	// Can't reset the breaker by calling `b.Report(nil)`. It's the probe's
	// job to reset the breaker.
	b.Report(nil)
	require.Error(t, b.Err())

	{
		// The probe finishes. Breaker is still open, so next time anyone
		// observes that, another probe is launched.
		done()
		report, done = nil, nil
		require.Error(t, b.Err())
		require.NotNil(t, report)
		require.NotNil(t, done)
	}

	{
		// The probe reports success this time. The breaker should reset and
		// stay that way.
		report(nil)
		require.NoError(t, b.Err())
		// The probe finishes.
		done()
		require.NoError(t, b.Err())
		require.NoError(t, b.Err())
	}
}

func TestBreakerV2Realistic(t *testing.T) {
	ctx := context.Background()
	s := stop.NewStopper()
	defer s.Stop(ctx)
	rnd, seed := randutil.NewTestPseudoRand()
	t.Logf("seed: %d", seed)
	const backoff = 1 * time.Millisecond
	b := NewBreakerV2(OptionsV2{
		Name: "testbreaker",
		ShouldTrip: func(err error) error {
			return err
		},
		EventHandler: testLogBridge(t),
		AsyncProbe: func(report func(error), done func()) {
			if err := s.RunAsyncTask(ctx, "probe", func(ctx context.Context) {
				defer done()

				for i := 1; ; i++ {
					select {
					case <-time.After(5 * backoff):
						// Unblock the breaker 33% of the time.
						if rnd.Intn(3) < 1 {
							report(nil /* err */)
							return
						}
						if rnd.Intn(2) < 1 {
							// Report new error 50% of the time.
							report(errors.Errorf("probe %d failed", i))
						}
					case <-s.ShouldQuiesce():
						return
					}
				}
			}); err != nil {
				report(err)
				done()
			}
		},
	})
	const numOpsPerWorker = 10
	const targetTripResetCycles = 5

	var numTrips int32 // atomic
	worker := func(ctx context.Context, t interface {
		Logf(string, ...interface{})
	}, idx int) error {
		defer t.Logf("w%d: worker done", idx)
		var doneOps int
		for i := 0; doneOps < numOpsPerWorker; i++ {
			runtime.Gosched()
			t.Logf("w%d: attempting op%d (attempt %d)", idx, doneOps, i+1)
			if err := b.Err(); err != nil {
				t.Logf("w%d: breaker open; backing off: %s", idx, err)
				time.Sleep(backoff)
				continue
			}
			// 16.5% chance of handing an error to the breaker, but only a few times
			// (so that the test is guaranteed to finish in a bounded amount of time
			// in practice despite possible parameter changes)
			if rnd.Intn(6) < 1 && atomic.AddInt32(&numTrips, 1) < targetTripResetCycles {
				err := errors.Errorf("error triggered by w%d/op%d", idx, doneOps)
				t.Logf("injecting error: %s", err)
				b.Report(err)
				continue
			}
			doneOps++
			t.Logf("w%d: op%d finished", idx, doneOps)
			i = 0
		}
		return nil
	}

	const numWorkers = 8
	g := ctxgroup.WithContext(ctx)
	for idx := 0; idx < numWorkers; idx++ {
		idx := idx // copy for the goroutine
		g.GoCtx(func(ctx context.Context) error {
			return s.RunTaskWithErr(ctx, "worker", func(ctx context.Context) error {
				return worker(ctx, t, idx)
			})
		})
	}
	require.NoError(t, g.Wait())
}
