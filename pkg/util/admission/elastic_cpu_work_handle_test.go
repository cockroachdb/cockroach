// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

func TestElasticCPUWorkHandle(t *testing.T) {
	overrideMu := struct {
		syncutil.Mutex
		running time.Duration
	}{}
	setRunning := func(running time.Duration) {
		overrideMu.Lock()
		defer overrideMu.Unlock()
		overrideMu.running = running
	}

	const allotment = 100 * time.Millisecond
	const zero = time.Duration(0)

	setRunning(zero)

	handle := newElasticCPUWorkHandle(roachpb.SystemTenantID, allotment)
	handle.testingOverrideRunningTime = func() time.Duration {
		overrideMu.Lock()
		defer overrideMu.Unlock()
		return overrideMu.running
	}

	{ // Assert on zero values.
		require.Equal(t, 0, handle.itersSinceLastCheck)
		require.Equal(t, 0, handle.itersUntilCheck)
		require.Equal(t, zero, handle.runningTimeAtLastCheck)
		require.Equal(t, zero, handle.differenceWithAllottedAtLastCheck)
	}

	{ // Invoke once; we should see internal state primed for future iterations.
		overLimit, difference := handle.OverLimit()
		expDifference := -allotment // we've not used anything, so we're at the difference is 0-allotment
		require.False(t, overLimit)
		require.Equal(t, expDifference, difference)
		require.Equal(t, 0, handle.itersSinceLastCheck)
		require.Equal(t, 1, handle.itersUntilCheck)
		require.Equal(t, zero, handle.runningTimeAtLastCheck)
		require.Equal(t, expDifference, handle.differenceWithAllottedAtLastCheck)
	}

	{ // Invoke while under the 1ms running duration. We should start doubling our itersUntilCheck count.
		setRunning(100 * time.Microsecond)
		expDifference := -(99*time.Millisecond + 900*time.Microsecond) // difference is negative, since we're under our allotment

		overLimit, difference := handle.OverLimit()
		require.False(t, overLimit)
		require.Equal(t, expDifference, difference)
		require.Equal(t, 0, handle.itersSinceLastCheck)
		require.Equal(t, 2, handle.itersUntilCheck)
		require.Equal(t, 100*time.Microsecond, handle.runningTimeAtLastCheck)
		require.Equal(t, expDifference, handle.differenceWithAllottedAtLastCheck)

		_, _ = handle.OverLimit()
		require.Equal(t, 1, handle.itersSinceLastCheck) // see increase of +1
		require.Equal(t, 2, handle.itersUntilCheck)
		require.Equal(t, 100*time.Microsecond, handle.runningTimeAtLastCheck)
		require.Equal(t, expDifference, handle.differenceWithAllottedAtLastCheck)

		_, _ = handle.OverLimit()
		require.Equal(t, 0, handle.itersSinceLastCheck) // see reset of value
		require.Equal(t, 4, handle.itersUntilCheck)     // see doubling of value
		require.Equal(t, 100*time.Microsecond, handle.runningTimeAtLastCheck)
		require.Equal(t, expDifference, handle.differenceWithAllottedAtLastCheck)
	}

	{ // Cross the 1ms running mark. Loop until we observe as much.
		setRunning(time.Millisecond + 100*time.Microsecond)            // set to 1.1 ms to estimate 4 iters since last check (at 100us)
		expDifference := -(98*time.Millisecond + 900*time.Microsecond) // difference is negative, since we're under our allotment

		internalIters := 0
		for {
			internalIters++

			overLimit, _ := handle.OverLimit()
			require.False(t, overLimit)
			if expDifference != handle.differenceWithAllottedAtLastCheck {
				continue
			}

			if internalIters > 4 {
				t.Fatalf("exceeded expected internal iteration count of 4")
			}
			break
		}
		require.Equal(t, 4, internalIters)
		require.Equal(t, 0, handle.itersSinceLastCheck) // see reset of value
		require.Equal(t, 4, handle.itersUntilCheck)     // see value remain static
		require.Equal(t, time.Millisecond+100*time.Microsecond, handle.runningTimeAtLastCheck)
		require.Equal(t, expDifference, handle.differenceWithAllottedAtLastCheck)
	}

	{ // Ensure steady estimation (4 iters per ms of running time) if iteration duration is steady.
		for i := 1; i <= 10; i++ {
			setRunning(handle.runningTimeAtLastCheck + time.Millisecond)

			internalIters := 0
			for {
				internalIters++

				overLimit, _ := handle.OverLimit()
				require.False(t, overLimit)
				if handle.itersSinceLastCheck == 0 {
					break
				}

				if internalIters > 4 {
					t.Fatalf("exceeded expected internal iteration count of 4")
				}
			}

			require.Equal(t, 4, internalIters)
			require.Equal(t, 0, handle.itersSinceLastCheck) // see reset of value
			require.Equal(t, 4, handle.itersUntilCheck)     // see value remain static
			require.Equal(t, time.Duration((int64(i+1)*time.Millisecond.Nanoseconds())+(100*time.Microsecond.Nanoseconds())), handle.runningTimeAtLastCheck)
		}
	}

	{ // We'll double our estimates again if we observe more iterations in 1ms of running time.
		for i := 0; i < 4; i++ {
			overLimit, _ := handle.OverLimit()
			require.False(t, overLimit)
		}

		require.Equal(t, 0, handle.itersSinceLastCheck) // see reset of value
		require.Equal(t, 8, handle.itersUntilCheck)     // see value double again
	}

	{
		setRunning(allotment + 50*time.Microsecond)
		expDifference := 50 * time.Microsecond // difference is finally positive, since we're over our allotment
		for i := 0; i < 7; i++ {
			overLimit, _ := handle.OverLimit()
			require.False(t, overLimit)
		}

		overLimit, difference := handle.OverLimit()
		require.True(t, overLimit)
		require.Equal(t, expDifference, difference)
	}
}

func TestElasticCPUWorkHandlePreWork(t *testing.T) {
	overrideMu := struct {
		syncutil.Mutex
		running time.Duration
	}{}
	setRunning := func(running time.Duration) {
		overrideMu.Lock()
		defer overrideMu.Unlock()
		overrideMu.running = running
	}

	const allotment = 100 * time.Millisecond
	const zero = time.Duration(0)

	setRunning(zero)

	handle := newElasticCPUWorkHandle(roachpb.SystemTenantID, allotment)
	handle.testingOverrideRunningTime = func() time.Duration {
		overrideMu.Lock()
		defer overrideMu.Unlock()
		return overrideMu.running
	}

	// Start the timer after already run for 450ms.
	setRunning(450 * time.Millisecond)
	handle.StartTimer()
	// All of it should count towards "pre-work" now.
	require.Equal(t, handle.preWork, 450*time.Millisecond)

	// Set the running time (effectively duration since the timer started, or
	// when the handle was initialized) to 70ms.
	setRunning(70 * time.Millisecond)

	// OverLimit() only considers time spent after doing pre-work in its boolean
	// return. Since our allotment was 100ms, of which we've used 70ms, we're
	// not over limit.
	overLimit, difference := handle.OverLimit()
	require.False(t, overLimit)
	// That said, the difference it returns does include all pre-work. So +450ms
	// doing pre-work, and -30ms from the unused allotment after the timer
	// started.
	require.Equal(t, difference, (450-30)*time.Millisecond)

	// Blow past the 100ms allotment after the timer started.
	setRunning(150 * time.Millisecond)
	// Since 150ms > 100ms, we're now over limit.
	overLimit, difference = handle.OverLimit()
	require.True(t, overLimit)
	// Pre-work is still counted in the difference we return. Also included is
	// how much over the allotment we ran (+50ms).
	require.Equal(t, difference, (450+50)*time.Millisecond)
}
