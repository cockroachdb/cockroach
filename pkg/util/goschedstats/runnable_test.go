// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	// Start 400 goroutines that never finish.
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
	expectedRunnable := n - runtime.GOMAXPROCS(0) + 1
	// We arbitrarily say that we expect no idle procs if the runnable count is
	// higher than GOMAXPROCS. Ideally, this should be expectedRunnable > 0, but
	// observations like the following suggest that that condition may be too
	// strict:
	// runnable: 394, max-procs: 10, idle-procs: 2
	//
	// Also, with n = 20, and running on my macbook, I have seen sequences like
	// runnable: 18, max-procs: 10, idle-procs: 6
	// runnable: 18, max-procs: 10, idle-procs: 6
	// runnable: 18, max-procs: 10, idle-procs: 6
	// runnable: 18, max-procs: 10, idle-procs: 6
	// runnable: 15, max-procs: 10, idle-procs: 3
	// runnable: 14, max-procs: 10, idle-procs: 2
	// runnable: 14, max-procs: 10, idle-procs: 2
	// runnable: 14, max-procs: 10, idle-procs: 2
	// runnable: 11, max-procs: 10, idle-procs: 0
	// Which suggest that it is not uncommon for idle-procs to be non-zero while
	// there are runnable goroutines.

	expectedNoIdle := expectedRunnable > runtime.GOMAXPROCS(0)
	testutils.SucceedsSoon(t, func() error {
		runnable, maxProcs, idleProcs := numRunnableGoroutines()
		fmt.Printf("runnable: %d, max-procs: %d, idle-procs: %d\n",
			runnable, maxProcs, idleProcs)
		if runnable < expectedRunnable {
			return fmt.Errorf("only %d runnable goroutines, expected %d", runnable, expectedRunnable)
		}
		if idleProcs > 0 && expectedNoIdle {
			return fmt.Errorf("%d procs are idle", idleProcs)
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
	idleProcs := 1
	numRunnable := func() (numRunnable int, numProcs int, numIdleProcs int) {
		return runnable, 1, idleProcs
	}
	var callbackSamplePeriod time.Duration
	var numCallbacks int
	cb := func(numRunnable int, numProcs int, numIdleProcs int, samplePeriod time.Duration) {
		require.Equal(t, runnable, numRunnable)
		require.Equal(t, 1, numProcs)
		require.Equal(t, idleProcs, numIdleProcs)
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
	idleProcs = 0
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
