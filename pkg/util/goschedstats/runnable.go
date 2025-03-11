// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goschedstats

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

// Enabled is true if the system has access to the internal goroutine statistics
// (i.e. if CockroachDB was built using our Go fork).
const Enabled = enabled

// CumulativeNormalizedRunnableGoroutines returns the sum (over all seconds
// since the program started) of the average number of runnable goroutines per
// GOMAXPROC.
//
// Runnable goroutines are goroutines which are ready to run but are waiting for
// an available process. Sustained high numbers of waiting goroutines are a
// potential indicator of high CPU saturation (overload).
//
// The number of runnable goroutines is sampled frequently, and an average is
// calculated and accumulated once per second.
//
// If Enabled is false, returns 0.
func CumulativeNormalizedRunnableGoroutines() float64 {
	return cumulativeNormalizedRunnableGoroutines()
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
// by passing the ID to UnregisterRunnableCountCallback.
//
// If Enabled is false, returns id=-1 and ok=false. The caller must handle this
// case because the callback will never fire.
func RegisterRunnableCountCallback(cb RunnableCountCallback) (id int64, ok bool) {
	return registerRunnableCountCallback(cb)
}

// RunnableCountCallback is called with the current value of runnable
// goroutines, GOMAXPROCS, and the current sampling period.
type RunnableCountCallback func(numRunnable int, numGoMaxProcs int, samplePeriod time.Duration)

// UnregisterRunnableCountCallback unregisters a callback previously registered
// with RegisterRunnableCountCallback.
func UnregisterRunnableCountCallback(id int64) {
	unregisterRunnableCountCallback(id)
}

// RegisterSettings provides a settings object that can be used to alter
// callback frequency.
func RegisterSettings(st *cluster.Settings) {
	registerSettings(st)
}

//lint:ignore U1000 unused
var alwaysUseShortSamplePeriodEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"goschedstats.always_use_short_sample_period.enabled",
	"when set to true, the system always does 1ms sampling of runnable queue lengths",
	false)
