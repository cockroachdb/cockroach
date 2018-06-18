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

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var (
	lastMinuteIncreaseThreshold = settings.RegisterIntSetting(
		"server.heap_profiler.last_minute_increase_threshold",
		"maximum increase of Rss(in bytes) in last minute beyond which heap"+
			" profile is triggered",
		1<<30, // 1GB
	)
	percentSystemMemoryThreshold = settings.RegisterFloatSetting(
		"server.heap_profiler.percent_system_memory_threshold",
		"percent of system memory beyond which if Rss increases, "+
			"then heap profile is triggered",
		85,
	)
	minProfileInterval = settings.RegisterDurationSetting(
		"server.heap_profiler.min_profile_interval",
		"minimum time difference between two heap profiles taken due to some"+
			" heuristic",
		time.Second*100,
	)
	maxNumPerHeuristic = settings.RegisterIntSetting(
		"server.heap_profiler.max_num_per_heuristic",
		"maximum number of profiles per heuristic to be kept. "+
			"Profiles with lower score are GC'ed, but latest profile is always kept",
		5,
	)
)

// RssVal stores Rss along with the Timestamp at which it was recorded.
type RssVal struct {
	Timestamp time.Time
	Rss       int64
}

type stats struct {
	rssValues         []RssVal
	numPreviousValues int
	systemMemory      int64
	lastProfileTime   time.Time
}

// insertValue inserts v at the end of o.rssValues if it contains less than
// numPreviousMemValues. if it alredy contains numPreviousMemValues,
// all the values are shifted by 1 to the left(first value is removed) and v is
// then inserted at the end. Something smarter like circular buffer can be used
// here if numPreviousMemValues needs to be increased.
func (s *stats) insertValue(mv RssVal) {
	if len(s.rssValues) < s.numPreviousValues {
		s.rssValues = append(s.rssValues, mv)
		return
	}
	for i := 1; i < s.numPreviousValues; i++ {
		s.rssValues[i-1] = s.rssValues[i]
	}
	s.rssValues[s.numPreviousValues-1] = mv
}

type heuristic struct {
	name   string
	isTrue func(s *stats, st *cluster.Settings) (score int64, isTrue bool)
}

// lastMinuteIncreaseHeuristic is true if difference between latest Rss and
// minimum Rss in last minute is more than lastMinuteIncreaseThreshold.
// score is the difference between latest Rss and minimum Rss in the last
// minute.
var lastMinuteIncreaseHeuristic = heuristic{
	name: "last_minute_increase",
	isTrue: func(s *stats, st *cluster.Settings) (score int64, isTrue bool) {
		minIdx := len(s.rssValues) - 1
		currentValue := s.rssValues[len(s.rssValues)-1]
		for i := len(s.rssValues) - 2; i >= 0 && currentValue.Timestamp.Sub(s.rssValues[i].Timestamp) <= time.Minute; i-- {
			if s.rssValues[minIdx].Rss > s.rssValues[i].Rss {
				minIdx = i
			}
		}
		delta := s.rssValues[len(s.rssValues)-1].Rss - s.rssValues[minIdx].Rss
		if delta > lastMinuteIncreaseThreshold.Get(&st.SV) {
			return delta, true
		}
		return 0, false
	},
}

// percentSystemMemoryHeuristic is true if latest Rss is more than
// percentSystemMemoryThreshold percent of system memory.
// score is the latest value of Rss.
var percentSystemMemoryHeuristic = heuristic{
	name: "percent_system_memory",
	isTrue: func(s *stats, st *cluster.Settings) (score int64, isTrue bool) {
		currentValue := s.rssValues[len(s.rssValues)-1].Rss
		if float64(currentValue)*100/float64(s.systemMemory) > percentSystemMemoryThreshold.Get(&st.SV) {
			return currentValue, true
		}
		return 0, false
	},
}

type heapProfiler struct {
	*stats
	heuristics      []heuristic
	rssChan         <-chan RssVal
	takeHeapProfile func(ctx context.Context, dir string, prefix string, suffix string)
	gcProfiles      func(ctx context.Context, dir, prefix string, maxCount int64)
	currentTime     func() time.Time
	dir             string
}

const memprof = "memprof."

func (o *heapProfiler) maybeTakeHeapProfile(ctx context.Context, st *cluster.Settings) {
	if o.currentTime().Sub(o.lastProfileTime) > minProfileInterval.Get(&st.SV) {
		for _, h := range o.heuristics {
			if score, isTrue := h.isTrue(o.stats, st); isTrue {
				prefix := memprof + h.name + "."
				const format = "2006-01-02T15_04_05.999"
				suffix := fmt.Sprintf("%018d_%s", score, o.currentTime().Format(format))
				o.takeHeapProfile(ctx, o.dir, prefix, suffix)
				o.lastProfileTime = o.currentTime()
				if o.gcProfiles != nil {
					o.gcProfiles(ctx, o.dir, prefix, maxNumPerHeuristic.Get(&st.SV))
				}
				return
			}
		}
	}
}

func (o *heapProfiler) getWorker(
	stopper *stop.Stopper, st *cluster.Settings,
) func(ctx context.Context) {
	return func(ctx context.Context) {
		for {
			select {
			case v := <-o.rssChan:
				o.insertValue(v)
				o.maybeTakeHeapProfile(ctx, st)
			case <-stopper.ShouldStop():
				return
			}
		}
	}
}

// NewWorker returns a function which should be run as a worker of s and is
// responsible for taking heap profile when an OOM situtation is detected
// using some heuristics.
// rssChan is the channel on which Rss values should be pushed as soon as
// they are measured.
// dir is the directory in which profiles are stored.
func NewWorker(
	rssChan <-chan RssVal,
	dir string,
	stopper *stop.Stopper,
	systemMemory int64,
	frequency time.Duration,
	st *cluster.Settings,
) (func(ctx context.Context), error) {
	dir = filepath.Join(dir, "heap_profiler")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	hp := heapProfiler{
		stats: &stats{
			systemMemory:      systemMemory,
			numPreviousValues: int(time.Minute/frequency) + 1,
		},
		heuristics:      []heuristic{percentSystemMemoryHeuristic, lastMinuteIncreaseHeuristic},
		rssChan:         rssChan,
		takeHeapProfile: takeHeapProfile,
		gcProfiles:      gcProfiles,
		currentTime:     timeutil.Now,
		dir:             dir,
	}
	return hp.getWorker(stopper, st), nil
}

// gcProfiles removes least score profile matching the specified prefix when the
// number of profiles is more than maxCount. Requires that the suffix used for
// the profiles indicates score such that sorting the filenames corresponds to
// ordering the profiles from oldest to newest.
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
	// remove latestProfile by shifting all the elements after it by 1 to the
	// left and removing the last element.
	for i := latestProfileIdx; i < len(files)-1; i++ {
		files[i] = files[i+1]
	}
	files = files[:len(files)-1]
	maxCount--
	var count int64
	for i := len(files) - 1; i >= 0; i-- {
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
