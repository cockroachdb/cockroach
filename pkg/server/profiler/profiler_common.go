// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package profiler

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// envMemprofInterval specifies how often the high-water mark value will
// be reset. Immediately after it is reset, a new profile will be taken.
//
// If the value is 0, the collection of profiles gets disabled.
var envMemprofInterval = func() time.Duration {
	dur := envutil.EnvOrDefaultDuration("COCKROACH_MEMPROF_INTERVAL", time.Hour)
	if dur <= 0 {
		// Instruction to disable.
		return 0
	}
	return dur
}

// timestampFormat is chosen to mimic that used by the log
// package. This is not a hard requirement though; the profiles are
// stored in a separate directory.
const timestampFormat = "2006-01-02T15_04_05.000"

type testingKnobs struct {
	dontWriteProfiles    bool
	maybeTakeProfileHook func(willTakeProfile bool)
	now                  func() time.Time
}

func zeroFloor() int64 { return 0 }

type profiler struct {
	// These fields need to be initialized at profiler creation time.

	store *profileStore
	// highWaterMarkFloor is the "zero value" for highWaterMark but might be
	// larger than zero. For example, we may only ever want to take a CPU profile
	// if CPU usage is high.
	//
	// The returned number may change from call to call.
	highWaterMarkFloor func() int64
	// resetInterval indicates the duration after which the high watermark resets
	// to the floor, i.e. after which a new profile may be taken assuming it
	// clears the floor threshold. This may change from call to call.
	resetInterval func() time.Duration
	knobs         testingKnobs

	// Internal state, does not need to be initialized.

	// lastProfileTime marks the time when we took the last profile.
	lastProfileTime time.Time
	// highWaterMark represents the maximum score that we've seen since
	// resetting the filed (which happens periodically).
	highWaterMark int64
}

func makeProfiler(
	store *profileStore, highWaterMarkBytesFloor func() int64, resetInterval func() time.Duration,
) profiler {
	return profiler{
		store:              store,
		highWaterMarkFloor: highWaterMarkBytesFloor,
		resetInterval:      resetInterval,
	}
}

func (o *profiler) now() time.Time {
	if o.knobs.now != nil {
		return o.knobs.now()
	}
	return timeutil.Now()
}

// args is optional and will be passed as is into takeProfileFn.
func (o *profiler) maybeTakeProfile(
	ctx context.Context,
	thresholdValue int64,
	takeProfileFn func(ctx context.Context, path string, args ...interface{}) bool,
	args ...interface{},
) {
	if o.resetInterval() == 0 {
		// Instruction to disable.
		return
	}

	now := o.now()
	// Check whether to reset the high watermark to the floor. This is the case if
	// the high water mark is now below the floor (which might change), or if
	// enough time has elapsed to reset the high water mark.
	if floor := o.highWaterMarkFloor(); o.highWaterMark < floor || now.Sub(o.lastProfileTime) >= o.resetInterval() {
		o.highWaterMark = floor
	}

	takeProfile := thresholdValue > o.highWaterMark
	if hook := o.knobs.maybeTakeProfileHook; hook != nil {
		hook(takeProfile)
	}
	if !takeProfile {
		return
	}

	o.highWaterMark = thresholdValue
	o.lastProfileTime = now

	if o.knobs.dontWriteProfiles {
		return
	}
	success := takeProfileFn(ctx, o.store.makeNewFileName(now, thresholdValue), args...)
	if success {
		// We only remove old files if the current dump was
		// successful. Otherwise, the GC may remove "interesting" files
		// from a previous crash.
		o.store.gcProfiles(ctx, now)
	}
}
