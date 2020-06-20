// Copyright 2018 The Cockroach Authors.
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
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// resetHighWaterMarkInterval specifies how often the high-water mark value will
// be reset. Immediately after it is reset, a new profile will be taken.
const resetHighWaterMarkInterval = time.Hour

type testingKnobs struct {
	dontWriteProfiles    bool
	maybeTakeProfileHook func(willTakeProfile bool)
	now                  func() time.Time
}

// HeapProfiler is used to take heap profiles.
//
// MaybeTakeProfile() is supposed to be called periodically. A profile is taken
// every time Go heap allocated bytes exceeds the previous high-water mark. The
// recorded high-water mark is also reset periodically, so that we take some
// profiles periodically.
// Profiles are also GCed periodically. The latest is always kept, and a couple
// of the ones with the largest heap are also kept.
type HeapProfiler struct {
	dir profileStore
	// lastProfileTime marks the time when we took the last profile.
	lastProfileTime time.Time
	// highwaterMarkBytes represents the maximum heap size that we've seen since
	// resetting the filed (which happens periodically).
	highwaterMarkBytes uint64

	knobs testingKnobs
}

// NewHeapProfiler creates a HeapProfiler. dir is the directory in which
// profiles are to be stored.
func NewHeapProfiler(dir string, st *cluster.Settings) (*HeapProfiler, error) {
	if dir == "" {
		return nil, errors.Errorf("need to specify dir for NewHeapProfiler")
	}
	hp := &HeapProfiler{
		dir: profileStore{dir: dir, st: st},
	}
	return hp, nil
}

// MaybeTakeProfile takes a heap profile if the heap is big enough.
func (o *HeapProfiler) MaybeTakeProfile(ctx context.Context, ms runtime.MemStats) {
	now := o.now()
	// If it's been too long since we took a profile, make sure we'll take one now.
	if now.Sub(o.lastProfileTime) > resetHighWaterMarkInterval {
		o.highwaterMarkBytes = 0
	}

	curHeap := ms.HeapAlloc
	takeProfile := curHeap > o.highwaterMarkBytes
	if hook := o.knobs.maybeTakeProfileHook; hook != nil {
		hook(takeProfile)
	}
	if !takeProfile {
		return
	}

	o.highwaterMarkBytes = curHeap
	o.lastProfileTime = now

	if o.knobs.dontWriteProfiles {
		return
	}
	path := o.dir.makeNewFileName(now, curHeap)
	success := takeHeapProfile(ctx, path)
	if success {
		// We only remove old files if the current heap dump was
		// successful. Otherwise, the GC may remove "interesting" files
		// from a previous crash.
		o.dir.gcProfiles(ctx, now)
	}
}

func (o *HeapProfiler) now() time.Time {
	if o.knobs.now != nil {
		return o.knobs.now()
	}
	return timeutil.Now()
}

// takeHeapProfile returns true if and only if the profile dump was
// taken successfully.
func takeHeapProfile(ctx context.Context, path string) (success bool) {
	// Try writing a go heap profile.
	f, err := os.Create(path)
	if err != nil {
		log.Warningf(ctx, "error creating go heap profile %s: %v", path, err)
		return false
	}
	defer f.Close()
	if err = pprof.WriteHeapProfile(f); err != nil {
		log.Warningf(ctx, "error writing go heap profile %s: %v", path, err)
		return false
	}
	return true
}
