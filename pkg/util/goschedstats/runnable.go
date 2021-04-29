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
	return float64(atomic.LoadUint64(&total)) * fromFixedPoint
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
	return float64(atomic.LoadUint64(&ewma)) * fromFixedPoint
}

// If you get a compilation error here, the Go version you are using is not
// supported by this package. Cross-check the structures in runtime_go1.15.go
// against those in the new Go's runtime, and if they are still accurate adjust
// the build tag in that file to accept the version. If they don't match, you
// will have to add a new version of that file.
var _ = numRunnableGoroutines

// We sample the number of runnable goroutines once per samplePeriod.
const samplePeriod = time.Millisecond

// We "report" the average value every reportingPeriod.
// Note: if this is changed from 1s, CumulativeNormalizedRunnableGoroutines()
// needs to be updated to scale the sum accordingly.
const reportingPeriod = time.Second

// For efficiency, we use "fixed point" arithmetic in the sampling loop: we
// scale up values and use integers.
const toFixedPoint = 65536
const fromFixedPoint = 1.0 / toFixedPoint

// total accumulates the sum of the number of runnable goroutines per CPU
// (averaged over a reporting period), multiplied by toFixedPoint.
var total uint64

// ewma keeps an exponentially weighted moving average of the runnable
// goroutines per CPU (averaged over a reporting period), multiplied by
// toFixedPoint.
// The EWMA coefficient is 0.5.
var ewma uint64

func init() {
	go func() {
		lastTime := timeutil.Now()
		// sum accumulates the sum of the number of runnable goroutines per CPU,
		// multiplied by toFixedPoint, for all samples since the last reporting.
		var sum uint64
		var numSamples int

		ticker := time.NewTicker(samplePeriod)
		// We keep local versions of "total" and "ewma" and we just Store the
		// updated values to the globals.
		var localTotal, localEWMA uint64
		for {
			t := <-ticker.C
			if t.Sub(lastTime) > reportingPeriod {
				if numSamples > 0 {
					// We want the average value over the reporting period, so we divide
					// by numSamples.
					newValue := sum / uint64(numSamples)
					localTotal += newValue
					atomic.StoreUint64(&total, localTotal)

					// ewma(t) = c * value(t) + (1 - c) * ewma(t-1)
					// We use c = 0.5.
					localEWMA = (newValue + localEWMA) / 2
					atomic.StoreUint64(&ewma, localEWMA)
				}
				lastTime = t
				sum = 0
				numSamples = 0
			}
			runnable, numProcs := numRunnableGoroutines()
			// The value of the sample is the ratio of runnable to numProcs (scaled
			// for fixed-point arithmetic).
			sum += uint64(runnable) * toFixedPoint / uint64(numProcs)
			numSamples++
		}
	}()
}

var _ = RecentNormalizedRunnableGoroutines
