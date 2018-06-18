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

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	// number of previous values of rss to be kept
	numPreviousMemValues = 10
	// maximum increase of rss in last minute beyond which heap profile is
	// triggered
	lastMinuteIncreaseThreshold = 1 << 30
	// percent of system memory beyond which if rss increases, then heap profile
	// is triggered
	percentSystemMemoryThreshold float64 = 50
	// minimum time difference between two heap profiles taken due to some
	// heuristic
	timeDifferenceBetweenProfiles = time.Second * 100
	// maximum number of profiles per heuristic to be kept. Profiles with lower
	// score are GC'ed
	maxProfilePerHeuristic = 5
)

type rssVal struct {
	timestamp time.Time
	rss       int64
}

type stats struct {
	rssValues    []rssVal
	systemMemory int64
	lastDumpTime time.Time
}

type heuristic struct {
	name   string
	isTrue func(s *stats) (score int64, isTrue bool)
}

// lastMinuteIncreaseHeuristic is true if difference between latest rss and
// minimum rss in last minute is more than lastMinuteIncreaseThreshold.
// score is the difference between latest rss and minimum rss in the last
// minute.
var lastMinuteIncreaseHeuristic = heuristic{
	name: "last_minute_increase",
	isTrue: func(s *stats) (score int64, isTrue bool) {
		minIdx := len(s.rssValues) - 1
		currentValue := s.rssValues[len(s.rssValues)-1]
		for i := len(s.rssValues) - 2; i >= 0 && currentValue.timestamp.Sub(s.rssValues[i].timestamp) <= time.Minute; i-- {
			if s.rssValues[minIdx].rss > s.rssValues[i].rss {
				minIdx = i
			}
		}
		if delta := s.rssValues[len(s.rssValues)-1].rss - s.rssValues[minIdx].rss; delta > lastMinuteIncreaseThreshold {
			return delta, true
		}
		return 0, false
	},
}

// percentSystemMemoryHeuristic is true if latest rss is more than
// percentSystemMemoryThreshold percent of system memory.
// score is the latest value of rss.
var percentSystemMemoryHeuristic = heuristic{
	name: "percent_system_memory",
	isTrue: func(s *stats) (score int64, isTrue bool) {
		currentValue := s.rssValues[len(s.rssValues)-1].rss
		if float64(currentValue)*100/float64(s.systemMemory) > percentSystemMemoryThreshold {
			return currentValue, true
		}
		return 0, false
	},
}

type heapProfiler struct {
	*stats
	heuristics                 []heuristic
	rssChan                    <-chan int64
	takeHeapProfile            func(ctx context.Context, dir string, prefix string, suffix string)
	dir                        string
	timeDifferenceBetweenDumps time.Duration
}

// insertValue inserts v at the end of o.rssValues if it contains less than
// numPreviousMemValues. if it alredy contains numPreviousMemValues,
// all the values are shifted by 1 to the left(first value is removed) and v is
// then inserted at the end. Something smarter like circular buffer can be used
// here if numPreviousMemValues needs to be increased.
func (o *heapProfiler) insertValue(v int64) {
	mv := rssVal{timestamp: timeutil.Now(), rss: v}
	if len(o.rssValues) < numPreviousMemValues {
		o.rssValues = append(o.rssValues, mv)
		return
	}
	for i := 1; i < numPreviousMemValues; i++ {
		o.rssValues[i-1] = o.rssValues[i]
	}
	o.rssValues[numPreviousMemValues-1] = mv
}

const memprof = "memprof."

func (o *heapProfiler) maybeTakeHeapProfile(ctx context.Context) {
	if timeutil.Since(o.lastDumpTime) > o.timeDifferenceBetweenDumps {
		for _, h := range o.heuristics {
			if score, isTrue := h.isTrue(o.stats); isTrue {
				prefix := memprof + h.name + "."
				const format = "2006-01-02T15_04_05.999"
				suffix := fmt.Sprintf("%018d_%s", score, timeutil.Now().Format(format))
				o.takeHeapProfile(ctx, filepath.Join(o.dir, h.name), prefix, suffix)
				o.lastDumpTime = timeutil.Now()
				return
			}
		}
	}
}

func (o *heapProfiler) getWorker(s *stop.Stopper) func(ctx context.Context) {
	return func(ctx context.Context) {
		for {
			select {
			case v := <-o.rssChan:
				o.insertValue(v)
				o.maybeTakeHeapProfile(ctx)
			case <-s.ShouldStop():
				return
			}
		}
	}
}

// NewWorker returns a function which should be run as a worker of s and is
// responsible for taking heap profile when an OOM situtation is detected
// using some heuristics.
// rssChan is the channel on which rss values should be pushed as soon as
// they are measured.
// dir is the directory in which profiles are stored.
func NewWorker(
	rssChan <-chan int64, dir string, s *stop.Stopper, systemMemory int64,
) (func(ctx context.Context), error) {
	heuristics := []heuristic{percentSystemMemoryHeuristic, lastMinuteIncreaseHeuristic}
	for _, h := range heuristics {
		if err := os.MkdirAll(filepath.Join(dir, h.name), 0755); err != nil {
			return nil, err
		}
	}
	hp := heapProfiler{
		stats:           &stats{systemMemory: systemMemory},
		heuristics:      heuristics,
		rssChan:         rssChan,
		takeHeapProfile: takeHeapProfile,
		dir:             dir,
		timeDifferenceBetweenDumps: timeDifferenceBetweenProfiles,
	}
	return hp.getWorker(s), nil
}

// gcProfiles removes least score profile matching the specified prefix when the
// number of profiles is more than maxCount. Requires that the suffix used for
// the profiles indicates score such that sorting the filenames corresponds to
// ordering the profiles from oldest to newest.
// Latest profile in the directory is not considered for GC.
func gcProfiles(dir, prefix string, maxCount int) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Warning(context.Background(), err)
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
	for i := latestProfileIdx; i < len(files); i++ {
		files[i] = files[i+1]
	}
	files = files[:len(files)-1]
	maxCount--
	var count int
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
			log.Info(context.Background(), err)
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
	gcProfiles(dir, prefix, maxProfilePerHeuristic)
}
