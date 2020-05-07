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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// resetHighWaterMarkInterval specifies how often the high-water mark value will
// be reset. Immediately after it is reset, a new profile will be taken.
const resetHighWaterMarkInterval = time.Hour

var (
	maxProfiles = settings.RegisterIntSetting(
		"server.heap_profile.max_profiles",
		"maximum number of profiles to be kept. "+
			"Profiles with lower score are GC'ed, but latest profile is always kept.",
		5,
	)
)

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
	dir string
	st  *cluster.Settings
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
		dir: dir,
		st:  st,
	}
	return hp, nil
}

// MaybeTakeProfile takes a heap profile if the heap is big enough.
func (o *HeapProfiler) MaybeTakeProfile(ctx context.Context, ms runtime.MemStats) {
	// If it's been too long since we took a profile, make sure we'll take one now.
	if o.now().Sub(o.lastProfileTime) > resetHighWaterMarkInterval {
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
	o.lastProfileTime = o.now()

	if o.knobs.dontWriteProfiles {
		return
	}
	const format = "2006-01-02T15_04_05.999"
	filePrefix := "memprof."
	fileName := fmt.Sprintf("%s%018d_%s", filePrefix, curHeap, o.now().Format(format))
	path := filepath.Join(o.dir, fileName)
	takeHeapProfile(ctx, path)
	o.gcProfiles(ctx, o.dir, filePrefix)
}

func (o *HeapProfiler) now() time.Time {
	if o.knobs.now != nil {
		return o.knobs.now()
	}
	return timeutil.Now()
}

// gcProfiles removes least score profile matching the specified prefix when the
// number of profiles is more than maxCount. Requires that the suffix used for
// the profiles indicates score such that sorting the filenames corresponds to
// ordering the profiles from least to max score.
// Latest profile in the directory is not considered for GC.
func (o *HeapProfiler) gcProfiles(ctx context.Context, dir, prefix string) {
	maxCount := maxProfiles.Get(&o.st.SV)
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Warningf(ctx, "%v", err)
		return
	}

	latestProfileIdx := 0
	for i, fi := range files {
		if fi.ModTime().UnixNano() > files[latestProfileIdx].ModTime().UnixNano() {
			latestProfileIdx = i
		}
	}
	maxCount-- // Since latest profile always needs to be kept
	var count int64
	for i := len(files) - 1; i >= 0; i-- {
		if i == latestProfileIdx {
			continue
		}
		f := files[i]
		if !f.Mode().IsRegular() {
			continue
		}
		if !strings.HasPrefix(f.Name(), prefix) {
			continue
		}
		count++
		if count <= maxCount {
			continue
		}
		if err := os.Remove(filepath.Join(dir, f.Name())); err != nil {
			log.Infof(ctx, "%v", err)
		}
	}
}

func takeHeapProfile(ctx context.Context, path string) {
	// Try writing a go heap profile.
	f, err := os.Create(path)
	if err != nil {
		log.Warningf(ctx, "error creating go heap profile %s", err)
		return
	}
	defer f.Close()
	if err = pprof.WriteHeapProfile(f); err != nil {
		log.Warningf(ctx, "error writing go heap profile %s: %s", path, err)
		return
	}
}
