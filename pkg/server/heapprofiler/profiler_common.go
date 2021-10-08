// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package heapprofiler

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// resetHighWaterMarkInterval specifies how often the high-water mark value will
// be reset. Immediately after it is reset, a new profile will be taken.
//
// If the value is 0, the collection of profiles gets disabled.
var resetHighWaterMarkInterval = func() time.Duration {
	dur := envutil.EnvOrDefaultDuration("COCKROACH_MEMPROF_INTERVAL", time.Hour)
	if dur <= 0 {
		// Instruction to disable.
		return 0
	}
	return dur
}()

// timestampFormat is chosen to mimic that used by the log
// package. This is not a hard requirement though; the profiles are
// stored in a separate directory.
const timestampFormat = "2006-01-02T15_04_05.000"

type testingKnobs struct {
	dontWriteProfiles    bool
	maybeTakeProfileHook func(willTakeProfile bool)
	now                  func() time.Time
}

type profiler struct {
	store *profileStore

	// lastProfileTime marks the time when we took the last profile.
	lastProfileTime time.Time
	// highwaterMarkBytes represents the maximum heap size that we've seen since
	// resetting the filed (which happens periodically).
	highwaterMarkBytes int64

	knobs testingKnobs
}

func (o *profiler) now() time.Time {
	if o.knobs.now != nil {
		return o.knobs.now()
	}
	return timeutil.Now()
}

func (o *profiler) maybeTakeProfile(
	ctx context.Context,
	thresholdValue int64,
	takeProfileFn func(ctx context.Context, path string) bool,
) {
	if resetHighWaterMarkInterval == 0 {
		// Instruction to disable.
		return
	}

	now := o.now()
	// If it's been too long since we took a profile, make sure we'll take one now.
	if now.Sub(o.lastProfileTime) >= resetHighWaterMarkInterval {
		o.highwaterMarkBytes = 0
	}

	takeProfile := thresholdValue > o.highwaterMarkBytes
	if hook := o.knobs.maybeTakeProfileHook; hook != nil {
		hook(takeProfile)
	}
	if !takeProfile {
		return
	}

	o.highwaterMarkBytes = thresholdValue
	o.lastProfileTime = now

	if o.knobs.dontWriteProfiles {
		return
	}
	success := takeProfileFn(ctx, o.store.makeNewFileName(now, thresholdValue))
	if success {
		// We only remove old files if the current dump was
		// successful. Otherwise, the GC may remove "interesting" files
		// from a previous crash.
		o.store.gcProfiles(ctx, now)
	}
}
