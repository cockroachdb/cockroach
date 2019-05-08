// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package heapprofiler

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

const minProfileInterval = time.Minute

var (
	systemMemoryThresholdFraction = settings.RegisterFloatSetting(
		"server.heap_profile.system_memory_threshold_fraction",
		"fraction of system memory beyond which if Rss increases, "+
			"then heap profile is triggered",
		.85,
	)
	maxProfiles = settings.RegisterIntSetting(
		"server.heap_profile.max_profiles",
		"maximum number of profiles to be kept. "+
			"Profiles with lower score are GC'ed, but latest profile is always kept.",
		5,
	)
)

type stats struct {
	memStats          base.MemStats
	systemMemoryBytes uint64
	lastProfileTime   time.Time

	now func() time.Time
}

// HeapProfiler is used to take heap profiles if an OOM situation is
// detected. It stores relevant functions and stats for heuristics to use.
type HeapProfiler struct {
	*stats
	takeHeapProfile func(ctx context.Context, dir string, prefix string, suffix string)
	gcProfiles      func(ctx context.Context, dir, prefix string, maxCount int64)
	dir             string
}

const memprof = "memprof."

// MaybeTakeProfile takes a heap profile if an OOM situation is detected using
// heuristics enabled in o. At max one profile is taken in a call of this
// function. This function is also responsible for updating stats in o.
func (o *HeapProfiler) MaybeTakeProfile(
	ctx context.Context, st *cluster.Settings, memStats base.MemStats,
) {
	o.memStats = memStats
	profileTaken := false
	if score, isTrue := o.fractionSystemMemoryHeuristic(st); isTrue {
		if !profileTaken {
			prefix := memprof
			const format = "2006-01-02T15_04_05.999"
			suffix := fmt.Sprintf("%018d_%s", score, o.now().Format(format))
			o.takeHeapProfile(ctx, o.dir, prefix, suffix)
			o.lastProfileTime = o.now()
			profileTaken = true
			if o.gcProfiles != nil {
				o.gcProfiles(ctx, o.dir, memprof, maxProfiles.Get(&st.SV))
			}
		}
	}
}

// fractionSystemMemoryHeuristic is true if latest (RSS - goIdle) is more than
// systemMemoryThresholdFraction of system memory. No new profile is taken if
// Rss has been above threshold since the last time profile was taken.
// At max one profile will be taken per minProfileInterval.
func (o *HeapProfiler) fractionSystemMemoryHeuristic(
	st *cluster.Settings,
) (score uint64, isTrue bool) {
	threshold := systemMemoryThresholdFraction.Get(&st.SV)
	curMemUse := o.stats.memStats.RSSBytes - o.stats.memStats.Go.GoIdle
	if float64(curMemUse)/float64(o.stats.systemMemoryBytes) < threshold {
		// Plenty of memory left in the system. No profile.
		return 0, false
	}

	if o.stats.now().Sub(o.stats.lastProfileTime) > minProfileInterval {
		// It's been a while since the last profile. Let's take a new one.
		return curMemUse, true
	}

	// Looks like we had another recent profile that was good enough. No need to
	// take a new one.
	return 0, false
}

// NewHeapProfiler returns a HeapProfiler which has
// systemMemoryThresholdFraction heuristic enabled. dir is the directory in
// which profiles are stored.
func NewHeapProfiler(dir string, systemMemoryBytes uint64) (*HeapProfiler, error) {
	if dir == "" {
		return nil, errors.New("directory to store profiles could not be determined")
	}
	dir = filepath.Join(dir, "heap_profiler")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	hp := &HeapProfiler{
		stats: &stats{
			systemMemoryBytes: systemMemoryBytes,
			now:               timeutil.Now,
		},
		takeHeapProfile: takeHeapProfile,
		gcProfiles:      gcProfiles,
		dir:             dir,
	}
	return hp, nil
}

// gcProfiles removes least score profile matching the specified prefix when the
// number of profiles is more than maxCount. Requires that the suffix used for
// the profiles indicates score such that sorting the filenames corresponds to
// ordering the profiles from least to max score.
// Latest profile in the directory is not considered for GC.
func gcProfiles(ctx context.Context, dir, prefix string, maxCount int64) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Warning(ctx, err)
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
			log.Info(ctx, err)
		}
	}
}

func takeHeapProfile(ctx context.Context, dir string, prefix string, suffix string) {
	path := filepath.Join(dir, prefix+suffix)
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
