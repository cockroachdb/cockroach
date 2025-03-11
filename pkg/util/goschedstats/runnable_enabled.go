// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This file contains the proper logic of the public functions in the
// goschedstats package. We only have access to the internal logic if we are
// using our fork which adds runtime.NumRunnableGoroutines().
//
//go:build bazel

package goschedstats

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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

// We sample the number of runnable goroutines once per samplePeriodShort or
// samplePeriodLong (if the system is underloaded). Using samplePeriodLong can
// cause sluggish response to a load spike, from the perspective of
// RunnableCountCallback implementers (admission control), so it is not ideal.
// We support this behavior only because we have observed 5-10% of cpu
// utilization on CockroachDB nodes that are doing no other work, even though
// 1ms polling (samplePeriodShort) is extremely cheap. The cause may be a poor
// interaction with processor idle state
// https://github.com/golang/go/issues/30740#issuecomment-471634471. See
// #66881.
//
// The use of underloadedRunnablePerProcThreshold does not provide sufficient
// protection against sluggish response in the admission control system, which
// uses these samples to adjust concurrency of request processing. So
// goschedstats.always_use_short_sample_period.enabled can be set to true to
// force this responsiveness.
const samplePeriodShort = time.Millisecond
const samplePeriodLong = 250 * time.Millisecond

// The system is underloaded if the number of runnable goroutines per proc is
// below this threshold. We have observed that steady workloads (like kv0),
// can have 50% cpu utilization and < 0.2 runnable goroutines per proc. We
// want to err on the side of a lower threshold since the samplePeriodShort
// regime allows admission control to react faster to fluctuations in runnable
// goroutines -- real world workloads sometimes have very spiky CPU
// utilization (see the graphs in
// https://github.com/cockroachdb/cockroach/issues/111125). So we set this to
// 0.1 runnable goroutine per proc.
const underloadedRunnablePerProcThreshold = 1 * toFixedPoint / 10

var alwaysUseShortSamplePeriodEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"goschedstats.always_use_short_sample_period.enabled",
	"when set to true, the system always does 1ms sampling of runnable queue lengths",
	false)

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
	st  *cluster.Settings
}

// RegisterRunnableCountCallback registers a callback to be run with the
// runnable and procs info, every 1ms, unless cpu load is very low (see the
// commentary for samplePeriodShort). This is exclusively for use by admission
// control that wants to react extremely quickly to cpu changes. Past
// experience in other systems (not CockroachDB) motivated not consuming a
// smoothed signal for admission control. The CockroachDB setting may possibly
// be different enough for that experience to not apply, but changing this to
// a smoothed value (say over 1s) should be done only after thorough load
// testing under adversarial load conditions (specifically, we need to react
// quickly to large drops in runnable due to blocking on IO, so that we don't
// waste cpu -- a workload that fluctuates rapidly between being IO bound and
// cpu bound could stress the usage of a smoothed signal).
//
// This function returns a unique ID for this callback which can be un-registered
// by passing the ID to UnregisterRunnableCountCallback. Notably, this function
// may return a negative number if we have no access to the internal Goroutine
// machinery (i.e. if we running a recent upstream version of Go; *not* our
// internal fork). In this case, the callback has not been registered.
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

// RegisterSettings provides a settings object that can be used to alter
// callback frequency.
func RegisterSettings(st *cluster.Settings) {
	callbackInfo.mu.Lock()
	defer callbackInfo.mu.Unlock()
	callbackInfo.st = st
}

func init() {
	go func() {
		sst := schedStatsTicker{
			lastTime:              timeutil.Now(),
			curPeriod:             samplePeriodShort,
			numRunnableGoroutines: runtime.NumRunnableGoroutines,
		}
		ticker := time.NewTicker(sst.curPeriod)
		for {
			t := <-ticker.C
			callbackInfo.mu.Lock()
			cbs := callbackInfo.cbs
			st := callbackInfo.st
			callbackInfo.mu.Unlock()
			sst.getStatsOnTick(t, cbs, st, ticker)
		}
	}()
}

// timeTickerInterface abstracts time.Ticker for testing.
type timeTickerInterface interface {
	Reset(d time.Duration)
}

// schedStatsTicker contains the local state maintained across stats collection
// ticks.
type schedStatsTicker struct {
	lastTime              time.Time
	curPeriod             time.Duration
	numRunnableGoroutines func() (numRunnable int, numProcs int)
	// sum accumulates the sum of the number of runnable goroutines per CPU,
	// multiplied by toFixedPoint, for all samples since the last reporting.
	sum uint64
	// numSamples is the number of samples since the last reporting.
	numSamples int
	// We keep local versions of "total" and "ewma" and we just Store the
	// updated values to the globals.
	localTotal, localEWMA uint64
}

// getStatsOnTick gets scheduler stats as the ticker has ticked. st can be nil.
func (s *schedStatsTicker) getStatsOnTick(
	t time.Time, cbs []callbackWithID, st *cluster.Settings, ticker timeTickerInterface,
) {
	if t.Sub(s.lastTime) > reportingPeriod {
		var avgValue uint64
		if s.numSamples > 0 {
			// We want the average value over the reporting period, so we divide
			// by numSamples.
			avgValue = s.sum / uint64(s.numSamples)
			s.localTotal += avgValue
			atomic.StoreUint64(&total, s.localTotal)

			// ewma(t) = c * value(t) + (1 - c) * ewma(t-1)
			// We use c = 0.5.
			s.localEWMA = (avgValue + s.localEWMA) / 2
			atomic.StoreUint64(&ewma, s.localEWMA)
		}
		nextPeriod := samplePeriodShort
		// Both the mean over the last 1s, and the exponentially weighted average
		// must be low for the system to be considered underloaded.
		if avgValue < underloadedRunnablePerProcThreshold &&
			s.localEWMA < underloadedRunnablePerProcThreshold &&
			(st == nil || !alwaysUseShortSamplePeriodEnabled.Get(&st.SV)) {
			// Underloaded, so switch to longer sampling period.
			nextPeriod = samplePeriodLong
		}
		// We switch the sample period only at reportingPeriod boundaries
		// since it ensures that all samples contributing to a reporting
		// period were at equal intervals (this is desirable since we average
		// them). It also naturally reduces the frequency at which we reset a
		// ticker.
		if nextPeriod != s.curPeriod {
			ticker.Reset(nextPeriod)
			s.curPeriod = nextPeriod
		}
		s.lastTime = t
		s.sum = 0
		s.numSamples = 0
	}
	runnable, numProcs := s.numRunnableGoroutines()
	for i := range cbs {
		cbs[i].RunnableCountCallback(runnable, numProcs, s.curPeriod)
	}
	// The value of the sample is the ratio of runnable to numProcs (scaled
	// for fixed-point arithmetic).
	s.sum += uint64(runnable) * toFixedPoint / uint64(numProcs)
	s.numSamples++
}
