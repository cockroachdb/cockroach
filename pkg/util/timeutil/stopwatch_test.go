// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timeutil_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestStopWatchStart makes sure that consequent calls to Start do not reset
// the internal startedAt time.
func TestStopWatchStart(t *testing.T) {
	timeSource := timeutil.NewTestTimeSource()
	w := timeutil.NewTestStopWatch(timeSource.Now)

	w.Start()
	timeSource.Advance()
	w.Start()
	timeSource.Advance()
	w.Stop()

	expected, actual := timeSource.Elapsed(), w.Elapsed()
	require.Equal(t, expected, actual)
}

// TestStopWatchStop makes sure that only the first call to Stop changes the
// state of the stop watch.
func TestStopWatchStop(t *testing.T) {
	timeSource := timeutil.NewTestTimeSource()
	w := timeutil.NewTestStopWatch(timeSource.Now)

	w.Start()
	timeSource.Advance()
	w.Stop()

	expected, actual := timeSource.Elapsed(), w.Elapsed()
	require.Equal(t, expected, actual)

	timeSource.Advance()
	w.Stop()
	require.Equal(t, actual, w.Elapsed(), "consequent call to StopWatch.Stop changed the elapsed time")
}

// TestStopWatchElapsed makes sure that the stop watch records the elapsed time
// correctly.
func TestStopWatchElapsed(t *testing.T) {
	timeSource := timeutil.NewTestTimeSource()
	w := timeutil.NewTestStopWatch(timeSource.Now)
	expected := time.Duration(10)

	w.Start()
	for i := int64(0); i < int64(expected); i++ {
		timeSource.Advance()
	}
	w.Stop()

	require.Equal(t, expected, w.Elapsed())
}

// TestStopWatchConcurrentUsage makes sure that the stop watch is safe for
// concurrent usage.
func TestStopWatchConcurrentUsage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const testDuration = time.Second
	const maxSleepTime = testDuration / 100
	const numGoroutines = 10

	// All operations that we can do on the stop watch.
	const (
		start int = iota
		stop
		elapsed
		numOperations
	)

	w := timeutil.NewStopWatch()
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		// Spin up multiple goroutines that will be using the stop watch
		// concurrently.
		go func() {
			defer wg.Done()
			rng, _ := randutil.NewPseudoRand()
			var timeSpent time.Duration
			for timeSpent < testDuration {
				// Sleep some random time, up to maxSleepTime.
				toSleep := time.Duration(float64(maxSleepTime) * rng.Float64())
				time.Sleep(toSleep)
				timeSpent += toSleep
				// Pick the operation randomly.
				switch operation := rng.Intn(numOperations); operation {
				case start:
					w.Start()
				case stop:
					w.Stop()
				case elapsed:
					_ = w.Elapsed()
				default:
					panic(fmt.Sprintf("unexpected operation %d", operation))
				}
			}
		}()
	}
	wg.Wait()
}
