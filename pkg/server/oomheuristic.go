// Copyright 2014 The Cockroach Authors.
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

package server

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
	numPreviousMemValues = 10      // number of previous values of rss to be kept
	minDeltaThreshold    = 1 << 30 // maximum difference between min of
	// all the stored values and current value of rss beyond which dump is triggered
	percentSystemMemoryThreshold float64 = 50 // percent of system memory
	// beyond which if rss increases, then mem dump is triggered
	timeDifferenceBetweenDumps = time.Second * 100 // minimum time difference between two
	// dumps taken due to some heuristic
	maxProfilePerHeuristic = 5
)

type rssVal struct {
	timestamp time.Time
	rss       int64
}

type heuristic struct {
	name   string
	subDir string
	isTrue func(o *oomHeuristicMemDumper) (score int64, isTrue bool)
}

var minDeltaHeuristic = heuristic{
	name:   "min_delta",
	subDir: "min_delta",
	isTrue: func(o *oomHeuristicMemDumper) (score int64, isTrue bool) {
		minIdx := 0
		for i, m := range o.rssValues {
			if o.rssValues[minIdx].rss > m.rss {
				minIdx = i
			}
		}
		if delta := o.rssValues[len(o.rssValues)-1].rss - o.rssValues[minIdx].
			rss; delta > minDeltaThreshold {
			return delta, true
		}
		return 0, false
	},
}

var percentSystemMemoryHeuristic = heuristic{
	name:   "percent_system_memory",
	subDir: "percent_system_memory",
	isTrue: func(o *oomHeuristicMemDumper) (score int64, isTrue bool) {
		currentValue := o.rssValues[len(o.rssValues)-1].rss
		if float64(currentValue)/float64(o.systemMemory) > percentSystemMemoryThreshold {
			return currentValue, true
		}
		return 0, false
	},
}

type oomHeuristicMemDumper struct {
	rssValues    []rssVal
	heuristics   []heuristic
	lastDumpTime time.Time
	systemMemory int64
	rssChan      <-chan int64
	takeHeapDump func(ctx context.Context, dir string, prefix string, suffix string)
	dir          string
}

// insertValue inserts v at the end of o.rssValues if it contains less than
// numPreviousMemValues. if it alredy contains numPreviousMemValues,
// all the values are shifted by 1 to the left(last value is removed) and v is
// then inserted at the end. Something smarted like circular buffer can be used
// here if numPreviousMemValues needs to be increased.
func (o *oomHeuristicMemDumper) insertValue(v int64) {
	mv := rssVal{timestamp: timeutil.Now(), rss: v}
	if len(o.rssValues) < numPreviousMemValues {
		o.rssValues = append(o.rssValues, mv)
		return
	}
	for i := 1; i < numPreviousMemValues; i++ {
		o.rssValues[i-1] = o.rssValues[i]
	}
	o.rssValues[numPreviousMemValues] = mv
}

const memprof = "memprof."

func (o *oomHeuristicMemDumper) maybeTakeHeapDump(ctx context.Context) {
	if timeutil.Since(o.lastDumpTime) > timeDifferenceBetweenDumps {
		for _, h := range o.heuristics {
			if score, isTrue := h.isTrue(o); isTrue {
				prefix := memprof + h.name + "."
				suffix := fmt.Sprintf("%018d", score)
				o.takeHeapDump(ctx, filepath.Join(o.dir, h.subDir), prefix, suffix)
				o.lastDumpTime = timeutil.Now()
				return
			}
		}
	}
}

func (o *oomHeuristicMemDumper) getWorker(s *stop.Stopper) func(ctx context.Context) {
	return func(ctx context.Context) {
		for {
			select {
			case v := <-o.rssChan:
				o.insertValue(v)
				o.maybeTakeHeapDump(ctx)
			case <-s.ShouldStop():
				return
			}
		}
	}
}

func newOOMHeuristicMemDumper(
	ctx context.Context, rssChan <-chan int64, dir string,
) (*oomHeuristicMemDumper, error) {
	systemMemory, err := GetTotalMemory(ctx)
	if err != nil {
		return nil, err
	}
	heuristics := []heuristic{percentSystemMemoryHeuristic, minDeltaHeuristic}
	for _, h := range heuristics {
		if err := os.MkdirAll(filepath.Join(dir, h.subDir), 0755); err != nil {
			return nil, err
		}
	}
	return &oomHeuristicMemDumper{
		heuristics:   heuristics,
		systemMemory: systemMemory,
		rssChan:      rssChan,
		takeHeapDump: takeHeapDump,
		dir:          dir,
	}, nil
}

// gcProfiles removes old profiles matching the specified prefix when the
// number of profiles is more than maxCount. Requires that the suffix used for
// the profiles indicates age (e.g. by using a date/timestamp suffix) such that
// sorting the filenames corresponds to ordering the profiles from oldest to
// newest. Copied from cli/start.go
func gcProfiles(dir, prefix string, maxCount int) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Warning(context.Background(), err)
		return
	}
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

func takeHeapDump(ctx context.Context, dir string, prefix string, suffix string) {
	path := filepath.Join(dir, prefix+suffix)
	// Try writing a go heap profile.
	f, err := os.Create(path)
	if err != nil {
		log.Warningf(ctx, "error creating go heap file %s", err)
		return
	}
	defer f.Close()
	if err = pprof.WriteHeapProfile(f); err != nil {
		log.Warningf(ctx, "error writing go heap %s: %s", path, err)
		return
	}
	gcProfiles(dir, prefix, maxProfilePerHeuristic)
}
