// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goschedstats

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestNumRunnableGoroutines(t *testing.T) {
	// Start 500 goroutines that never finish.
	const n = 400
	for i := 0; i < n; i++ {
		go func(i int) {
			a := 1
			for x := 0; x >= 0; x++ {
				a = a*13 + x
			}
		}(i)
	}
	// When we run, we expect at most GOMAXPROCS-1 of the n goroutines to be
	// running, with the rest waiting.
	expected := n - runtime.GOMAXPROCS(0) + 1
	testutils.SucceedsSoon(t, func() error {
		if n, _ := numRunnableGoroutines(); n < expected {
			return fmt.Errorf("only %d runnable goroutines, expected %d", n, expected)
		}
		return nil
	})
}

type testTimeTicker struct {
	numResets         int
	lastResetDuration time.Duration
}

func (t *testTimeTicker) Reset(d time.Duration) {
	t.numResets++
	t.lastResetDuration = d
}

func TestSchedStatsTicker(t *testing.T) {
	runnable := 0
	numRunnable := func() (numRunnable int, numProcs int) {
		return runnable, 1
	}
	var callbackSamplePeriod time.Duration
	var numCallbacks int
	cb := func(numRunnable int, numProcs int, samplePeriod time.Duration) {
		require.Equal(t, runnable, numRunnable)
		require.Equal(t, 1, numProcs)
		callbackSamplePeriod = samplePeriod
		numCallbacks++
	}
	cbs := []callbackWithID{{cb, 0}}
	now := timeutil.UnixEpoch
	startTime := now
	sst := schedStatsTicker{
		lastTime:              now,
		curPeriod:             samplePeriodShort,
		numRunnableGoroutines: numRunnable,
	}
	tt := testTimeTicker{}
	// Tick every 1ms until the reportingPeriod has elapsed.
	for i := 1; ; i++ {
		now = now.Add(samplePeriodShort)
		sst.getStatsOnTick(now, cbs, &tt)
		if now.Sub(startTime) <= reportingPeriod {
			// No reset of the time ticker.
			require.Equal(t, 0, tt.numResets)
			// Each tick causes a callback.
			require.Equal(t, i, numCallbacks)
			require.Equal(t, samplePeriodShort, callbackSamplePeriod)
		} else {
			break
		}
	}
	// Since underloaded, the time ticker is reset to samplePeriodLong, and this
	// period is provided to the latest callback.
	require.Equal(t, 1, tt.numResets)
	require.Equal(t, samplePeriodLong, tt.lastResetDuration)
	require.Equal(t, samplePeriodLong, callbackSamplePeriod)
	// Increase load so no longer underloaded.
	runnable = 2
	startTime = now
	tt.numResets = 0
	for i := 1; ; i++ {
		now = now.Add(samplePeriodLong)
		sst.getStatsOnTick(now, cbs, &tt)
		if now.Sub(startTime) <= reportingPeriod {
			// No reset of the time ticker.
			require.Equal(t, 0, tt.numResets)
			// Each tick causes a callback.
			require.Equal(t, samplePeriodLong, callbackSamplePeriod)
		} else {
			break
		}
	}
	// No longer underloaded, so the time ticker is reset to samplePeriodShort,
	// and this period is provided to the latest callback.
	require.Equal(t, 1, tt.numResets)
	require.Equal(t, samplePeriodShort, tt.lastResetDuration)
	require.Equal(t, samplePeriodShort, callbackSamplePeriod)
}
