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

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
// supported by this package. Cross-check the structures in runtime_go1.16.go
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

// RunnableCountCallback is provided the current value of runnable goroutines,
// and GOMAXPROCS.
type RunnableCountCallback func(numRunnable int, numProcs int)

type callbackWithID struct {
	RunnableCountCallback
	id int64
}

var callbackInfo struct {
	mu syncutil.Mutex
	id int64
	// Multiple cbs are used only for tests which can run multiple CockroachDB
	// nodes in a process.
	cbs []callbackWithID
}

// RegisterRunnableCountCallback registers a callback to be run with the
// runnable and procs info every 1ms. This is exclusively for use by admission
// control that wants to react extremely quickly to cpu changes. Past
// experience in other systems (not CockroachDB) motivated not consuming a
// smoothed signal for admission control. The CockroachDB setting may possibly
// be different enough for that experience to not apply, but changing this to
// a smoothed value (say over 1s) should be done only after thorough load
// testing under adversarial load conditions (specifically, we need to react
// quickly to large drops in runnable due to blocking on IO, so that we don't
// waste cpu -- a workload that fluctuates rapidly between being IO bound and
// cpu bound could stress the usage of a smoothed signal).
func RegisterRunnableCountCallback(cb RunnableCountCallback) (id int64) {
	callbackInfo.mu.Lock()
	defer callbackInfo.mu.Unlock()
	id = callbackInfo.id
	callbackInfo.id++
	callbackInfo.cbs = append(callbackInfo.cbs, callbackWithID{
		RunnableCountCallback: cb,
		id:                    id,
	})
	return id
}

// UnregisterRunnableCountCallback unregisters the callback to be run with the
// runnable and procs info.
func UnregisterRunnableCountCallback(id int64) {
	callbackInfo.mu.Lock()
	defer callbackInfo.mu.Unlock()
	// Unregistration only happens in test settings, so simply copy to a new
	// slice so that the existing slice is only appended to.
	newCBs := []callbackWithID(nil)
	for i := range callbackInfo.cbs {
		if callbackInfo.cbs[i].id == id {
			continue
		}
		newCBs = append(newCBs, callbackInfo.cbs[i])
	}
	if len(newCBs)+1 != len(callbackInfo.cbs) {
		panic(errors.AssertionFailedf("unexpected unregister: new count %d, old count %d",
			len(newCBs), len(callbackInfo.cbs)))
	}
	callbackInfo.cbs = newCBs
}

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
			callbackInfo.mu.Lock()
			cbs := callbackInfo.cbs
			callbackInfo.mu.Unlock()
			for i := range cbs {
				cbs[i].RunnableCountCallback(runnable, numProcs)
			}
			// The value of the sample is the ratio of runnable to numProcs (scaled
			// for fixed-point arithmetic).
			sum += uint64(runnable) * toFixedPoint / uint64(numProcs)
			numSamples++
		}
	}()
}

var _ = RecentNormalizedRunnableGoroutines
