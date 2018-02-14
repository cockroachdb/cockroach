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

package workload

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/codahale/hdrhistogram"
)

const (
	minLatency = 100 * time.Microsecond
	maxLatency = 10 * time.Second
)

// Watch is a named histogram for use in Operations. It is threadsafe but
// intended to be thread-local.
type Watch struct {
	name string
	mu   struct {
		syncutil.Mutex
		numOps int64
		hist   *hdrhistogram.WindowedHistogram
	}
}

func newWatch(name string) *Watch {
	w := &Watch{name: name}
	w.mu.hist = hdrhistogram.NewWindowed(
		1, minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	return w
}

// Record saves a new datapoint and should be called once per logical operation.
func (w *Watch) Record(elapsed time.Duration) {
	w.mu.Lock()
	if elapsed < minLatency {
		elapsed = minLatency
	} else if elapsed > maxLatency {
		elapsed = maxLatency
	}
	err := w.mu.hist.Current.RecordValue(elapsed.Nanoseconds())
	w.mu.numOps++
	w.mu.Unlock()

	if err != nil {
		panic(fmt.Sprintf(`%s: recording value: %s`, w.name, err))
	}
}

// tick resets the current histogram to a new "period". The old one's data
// should be saved via the closure argument.
func (w *Watch) tick(fn func(numOps int64, h *hdrhistogram.Histogram)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	m := w.mu.hist.Merge()
	w.mu.hist.Rotate()
	fn(w.mu.numOps, m)
}

// WatchRegistry is a thread-safe enclosure for a (possibly large) number of
// named histograms. It allows for "tick"ing them periodically to reset the
// counts and also supports aggregations.
type WatchRegistry struct {
	mu struct {
		syncutil.Mutex
		registered []*Watch
	}

	start      time.Time
	cumulative map[string]*hdrhistogram.Histogram
	prevTick   map[string]WatchTick
}

// NewWatchRegistry returns an initialized WatchRegistry.
func NewWatchRegistry() *WatchRegistry {
	return &WatchRegistry{
		start:      timeutil.Now(),
		cumulative: make(map[string]*hdrhistogram.Histogram),
		prevTick:   make(map[string]WatchTick),
	}
}

// GetHandle returns a thread-local handle for creating and registering watches.
func (w *WatchRegistry) GetHandle() *Watches {
	watches := &Watches{reg: w}
	watches.mu.watches = make(map[string]*Watch)
	return watches
}

// Tick aggregates all registered watches, grouped by name. It is expected to be
// called periodially from one goroutine.
func (w *WatchRegistry) Tick(fn func(WatchTick)) {
	w.mu.Lock()
	registered := append([]*Watch(nil), w.mu.registered...)
	w.mu.Unlock()

	merged := make(map[string]*hdrhistogram.Histogram)
	totalOps := make(map[string]int64)
	for _, watch := range registered {
		watch.tick(func(numOps int64, h *hdrhistogram.Histogram) {
			// TODO(dan): It really seems like we should be able to use
			// `h.TotalCount()` for the number of operations but for some reason
			// that doesn't line up in practice. Investigate.
			totalOps[watch.name] += numOps
			if m, ok := merged[watch.name]; ok {
				m.Merge(h)
			} else {
				merged[watch.name] = h
			}
		})
	}

	// TODO(dan): Do this in a stable order (probably alphabetical).
	for name, mergedHist := range merged {
		if _, ok := w.cumulative[name]; !ok {
			w.cumulative[name] = hdrhistogram.New(
				minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
		}
		w.cumulative[name].Merge(mergedHist)

		prevTick, ok := w.prevTick[name]
		if !ok {
			prevTick.start = w.start
		}
		now := timeutil.Now()
		w.prevTick[name] = WatchTick{
			Name:       name,
			Hist:       merged[name],
			Cumulative: w.cumulative[name],
			Ops:        totalOps[name],
			LastOps:    prevTick.Ops,
			Elapsed:    now.Sub(prevTick.start),
			start:      now,
		}
		fn(w.prevTick[name])
	}
}

// Watches is a thread-local handle for creating and registering watches.
type Watches struct {
	reg *WatchRegistry
	mu  struct {
		syncutil.Mutex
		watches map[string]*Watch
	}
}

// Get returns a watch with the given name, creating and registering it if
// necessary. The result is cached, so no need to cache it in the
// workload.
func (w *Watches) Get(name string) *Watch {
	w.mu.Lock()
	watch, ok := w.mu.watches[name]
	if !ok {
		watch = newWatch(name)
		w.mu.watches[name] = watch
	}
	w.mu.Unlock()

	if !ok {
		w.reg.mu.Lock()
		w.reg.mu.registered = append(w.reg.mu.registered, watch)
		w.reg.mu.Unlock()
	}

	return watch
}

// WatchTick is an aggregation of ticking all watches in a WatchRegistry with a
// given name.
type WatchTick struct {
	// Name is the name given to the watches represented by this tick.
	Name string
	// Hist is the merged result of the represented histgrams for this tick.
	Hist *hdrhistogram.Histogram
	// Cumulative is the merged result of the represented histgrams for all
	// time.
	Cumulative *hdrhistogram.Histogram
	// Ops is the total number of `Record` calls for all represented watches.
	Ops int64
	// LastOps is the value of Ops for the last tick.
	LastOps int64
	// Elapsed is the amount of time since the last tick.
	Elapsed time.Duration

	start time.Time
}
