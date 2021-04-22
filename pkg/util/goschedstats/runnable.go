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
	"runtime"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// CumulativeNormalizedRunnableGoroutines returns the sum, over all seconds
// since the program started, of the average number of runnable goroutines per
// GOMAXPROC.
//
// Runnable goroutines are goroutines which are ready to run but are waiting for
// an available process. Sustained high numbers of waiting goroutines are a
// potential indicator of high CPU saturation (overload).
//
// The number of runnable goroutines is sampled frequently, and an average is
// calculated and accumulated once per second.
func CumulativeNormalizedRunnableGoroutines() float64 {
	return normalize(atomic.LoadUint64(&total))
}

// RecentNormalizedRunnableGoroutines returns a recent average of the number of
// runnable goroutines per GOMAXPROC.
//
// Runnable goroutines are goroutines which are ready to run but are waiting for
// an available process. Sustained high numbers of waiting goroutines are a
// potential indicator of high CPU saturation (overload).
//
// The number of runnable goroutines is sampled frequently, and an average is
// calculated once per second. This function returns an exponentially weighted
// moving average of these values.
func RecentNormalizedRunnableGoroutines() float64 {
	return normalize(atomic.LoadUint64(&ewma))
}

// normalize converts a raw value to a normalized value by applying the scale
// and averaging over GOMAXPROCS.
func normalize(rawValue uint64) float64 {
	procs := runtime.GOMAXPROCS(0)
	return float64(rawValue) * invScale / float64(procs)
}

// If you get a compilation error here, the Go version you are using is not
// supported by this package. Cross-check the structures in runtime_go1.15.go
// against those in the new Go's runtime, and if they are still accurate adjust
// the build tag in that file to accept the version. If they don't match, you
// will have to add a new version of that file.
var _ = numRunnableGoroutines

// We sample the number of runnable goroutines once per samplePeriod.
const samplePeriod = time.Millisecond

// We "report" the average value seen every reportingPeriod.
const reportingPeriod = time.Second

const scale = 1000
const invScale = 1.0 / scale

// total accumulates the sum of the average number of runnable goroutines per
// reportingPeriod, multiplied by scale.
var total uint64

// ewma keeps an exponentially weighted moving average of the runnable
// goroutines per reportingPeriod, multiplied by scale.
// The EWMA coefficient is 0.5.
var ewma uint64

func init() {
	go func() {
		lastTime := timeutil.Now()
		sum := numRunnableGoroutines()
		numSamples := 1

		ticker := time.NewTicker(samplePeriod)
		// We keep local versions of "total" and "ewma" and we just Store the
		// current values to the globals.
		var localTotal, localEWMA uint64
		for {
			t := <-ticker.C
			if t.Sub(lastTime) > reportingPeriod || t.Before(lastTime) {
				if numSamples > 0 {
					lastVal := uint64(sum * scale / numSamples)
					localTotal += lastVal
					atomic.StoreUint64(&total, localTotal)

					// ewma(t) = c * value(t) + (1 - c) * ewma(t-1)
					// We use c = 0.5.
					localEWMA = (lastVal + localEWMA) / 2
					atomic.StoreUint64(&ewma, localEWMA)
				}
				lastTime = t
				sum = 0
				numSamples = 0
			}
			sum += numRunnableGoroutines()
			numSamples++
		}
	}()
}

var _ = RecentNormalizedRunnableGoroutines
