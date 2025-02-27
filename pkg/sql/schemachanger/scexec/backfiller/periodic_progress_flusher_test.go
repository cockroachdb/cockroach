// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backfiller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeBackfillFlusher struct {
	flushFractionCompleted func(context.Context) error
	flushCheckpoint        func(context.Context) error
}

func (f fakeBackfillFlusher) FlushCheckpoint(ctx context.Context) error {
	return f.flushCheckpoint(ctx)
}

func (f fakeBackfillFlusher) FlushFractionCompleted(ctx context.Context) error {
	return f.flushFractionCompleted(ctx)
}

var _ scexec.BackfillerProgressFlusher = (*fakeBackfillFlusher)(nil)

func TestPeriodicBackfillProgressFlusher(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	ts := timeutil.NewManualTime(t0)
	var checkpointInterval, fractionInterval atomic.Value
	getDuration := func(v *atomic.Value) func() time.Duration {
		return func() time.Duration { return v.Load().(time.Duration) }
	}
	checkpointInterval.Store(time.Minute)
	fractionInterval.Store(time.Second)
	pt := &periodicProgressFlusher{
		clock:              ts,
		checkpointInterval: getDuration(&checkpointInterval),
		fractionInterval:   getDuration(&fractionInterval),
	}
	var checkpointFlushes, fractionFlushes int64
	injectFlushError := atomic.Bool{}
	incrementFunc := func(p *int64) func(context.Context) error {
		return func(ctx context.Context) error {
			if injectFlushError.Load() {
				return errors.New("injected error")
			}
			atomic.AddInt64(p, 1)
			return nil
		}
	}
	flusher := &fakeBackfillFlusher{
		flushFractionCompleted: incrementFunc(&fractionFlushes),
		flushCheckpoint:        incrementFunc(&checkpointFlushes),
	}

	stop := pt.StartPeriodicUpdates(context.Background(), flusher)
	waitForTimers := func(t *testing.T, exp []time.Time) {
		testutils.SucceedsSoon(t, func() error {
			if got := ts.Timers(); !assert.ElementsMatch(noopT{}, exp, got) {
				return errors.Errorf("expected %v, got %v", exp, got)
			}
			return nil
		})
	}

	type step = func(t *testing.T)
	waitForTimersStep := func(exp ...time.Duration) step {
		times := make([]time.Time, len(exp))
		for i, d := range exp {
			times[i] = t0.Add(d)
		}
		return func(t *testing.T) { waitForTimers(t, times) }
	}
	checkCountsStep := func(checkpoints, fractions int64) step {
		return func(t *testing.T) {
			require.Equal(t, fractions, atomic.LoadInt64(&fractionFlushes))
			require.Equal(t, checkpoints, atomic.LoadInt64(&checkpointFlushes))
		}
	}
	advanceStep := func(to time.Duration) step {
		return func(t *testing.T) { ts.AdvanceTo(t0.Add(to)) }
	}
	for _, s := range []step{
		waitForTimersStep(time.Second, time.Minute),
		advanceStep(1500 * time.Millisecond),

		// Wait for the next timer to be set before ensuring write was called.
		waitForTimersStep(2500*time.Millisecond, time.Minute),
		checkCountsStep(0, 1),

		// Changing the interval won't take effect until the next iteration.
		func(t *testing.T) { checkpointInterval.Store(time.Second) },

		// Trigger both loops to run and wait for the new timers.
		advanceStep(time.Minute),
		waitForTimersStep(61*time.Second, 61*time.Second),
		checkCountsStep(1, 2),

		// Inject errors into the flush callbacks. The periodic flusher should
		// not stop.
		func(t *testing.T) { injectFlushError.Store(true) },
		advanceStep(time.Minute + 5*time.Second),
		waitForTimersStep(66*time.Second, 66*time.Second),
		checkCountsStep(1, 2),

		// Stop injecting errors and make sure the progress gets updated again.
		func(t *testing.T) { injectFlushError.Store(false) },
		advanceStep(time.Minute + 10*time.Second),
		waitForTimersStep(71*time.Second, 71*time.Second),
		checkCountsStep(2, 3),

		// Ensure stopping works.
		func(t *testing.T) { stop() },
		waitForTimersStep(),

		// Ensure stopping is idempotent.
		func(t *testing.T) { stop() },
	} {
		t.Run("", s)
	}
}

// noopT enables use of assert/require without actually failing the test.
type noopT struct{}

func (n noopT) Errorf(format string, args ...interface{}) {}
func (n noopT) FailNow()                                  {}
