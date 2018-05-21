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
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/codahale/hdrhistogram"
)

const (
	sigFigs    = 1
	minLatency = 100 * time.Microsecond
	maxLatency = 100 * time.Second
)

func newHistogram() *hdrhistogram.Histogram {
	return hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), sigFigs)
}

// NamedHistogram is a named histogram for use in Operations. It is threadsafe
// but intended to be thread-local.
type NamedHistogram struct {
	name string
	mu   struct {
		syncutil.Mutex
		current *hdrhistogram.Histogram
	}
}

func newNamedHistogram(name string) *NamedHistogram {
	w := &NamedHistogram{name: name}
	w.mu.current = newHistogram()
	return w
}

// Record saves a new datapoint and should be called once per logical operation.
func (w *NamedHistogram) Record(elapsed time.Duration) {
	if elapsed < minLatency {
		elapsed = minLatency
	} else if elapsed > maxLatency {
		elapsed = maxLatency
	}

	w.mu.Lock()
	err := w.mu.current.RecordValue(elapsed.Nanoseconds())
	w.mu.Unlock()

	if err != nil {
		// Note that a histogram only drops recorded values that are out of range,
		// but we clamp the latency value to the configured range to prevent such
		// drops. This code path should never happen.
		panic(fmt.Sprintf(`%s: recording value: %s`, w.name, err))
	}
}

// tick resets the current histogram to a new "period". The old one's data
// should be saved via the closure argument.
func (w *NamedHistogram) tick(fn func(h *hdrhistogram.Histogram)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	h := w.mu.current
	w.mu.current = newHistogram()
	fn(h)
}

// HistogramRegistry is a thread-safe enclosure for a (possibly large) number of
// named histograms. It allows for "tick"ing them periodically to reset the
// counts and also supports aggregations.
type HistogramRegistry struct {
	mu struct {
		syncutil.Mutex
		registered []*NamedHistogram
	}

	start      time.Time
	cumulative map[string]*hdrhistogram.Histogram
	prevTick   map[string]time.Time
}

// NewHistogramRegistry returns an initialized HistogramRegistry.
func NewHistogramRegistry() *HistogramRegistry {
	return &HistogramRegistry{
		start:      timeutil.Now(),
		cumulative: make(map[string]*hdrhistogram.Histogram),
		prevTick:   make(map[string]time.Time),
	}
}

// GetHandle returns a thread-local handle for creating and registering
// NamedHistograms.
func (w *HistogramRegistry) GetHandle() *Histograms {
	hists := &Histograms{reg: w}
	hists.mu.hists = make(map[string]*NamedHistogram)
	return hists
}

// Tick aggregates all registered histograms, grouped by name. It is expected to
// be called periodically from one goroutine.
func (w *HistogramRegistry) Tick(fn func(HistogramTick)) {
	w.mu.Lock()
	registered := append([]*NamedHistogram(nil), w.mu.registered...)
	w.mu.Unlock()

	merged := make(map[string]*hdrhistogram.Histogram)
	var names []string
	for _, hist := range registered {
		hist.tick(func(h *hdrhistogram.Histogram) {
			if m, ok := merged[hist.name]; ok {
				m.Merge(h)
			} else {
				merged[hist.name] = h
				names = append(names, hist.name)
			}
		})
	}

	now := timeutil.Now()
	sort.Strings(names)
	for _, name := range names {
		mergedHist := merged[name]
		if _, ok := w.cumulative[name]; !ok {
			w.cumulative[name] = newHistogram()
		}
		w.cumulative[name].Merge(mergedHist)

		prevTick, ok := w.prevTick[name]
		if !ok {
			prevTick = w.start
		}
		w.prevTick[name] = now
		fn(HistogramTick{
			Name:       name,
			Hist:       merged[name],
			Cumulative: w.cumulative[name],
			Elapsed:    now.Sub(prevTick),
			Now:        now,
		})
	}
}

// Histograms is a thread-local handle for creating and registering
// NamedHistograms.
type Histograms struct {
	reg *HistogramRegistry
	mu  struct {
		syncutil.Mutex
		hists map[string]*NamedHistogram
	}
}

// Get returns a NamedHistogram with the given name, creating and registering it
// if necessary. The result is cached, so no need to cache it in the workload.
func (w *Histograms) Get(name string) *NamedHistogram {
	w.mu.Lock()
	hist, ok := w.mu.hists[name]
	if !ok {
		hist = newNamedHistogram(name)
		w.mu.hists[name] = hist
	}
	w.mu.Unlock()

	if !ok {
		w.reg.mu.Lock()
		w.reg.mu.registered = append(w.reg.mu.registered, hist)
		w.reg.mu.Unlock()
	}

	return hist
}

// HistogramTick is an aggregation of ticking all histograms in a
// HistogramRegistry with a given name.
type HistogramTick struct {
	// Name is the name given to the histograms represented by this tick.
	Name string
	// Hist is the merged result of the represented histograms for this tick.
	// Hist.TotalCount() is the number of operations that occurred for this tick.
	Hist *hdrhistogram.Histogram
	// Cumulative is the merged result of the represented histograms for all
	// time. Cumulative.TotalCount() is the total number of operations that have
	// occurred over all time.
	Cumulative *hdrhistogram.Histogram
	// Elapsed is the amount of time since the last tick.
	Elapsed time.Duration
	// Now is the time at which the tick was gathered. It covers the period
	// [Now-Elapsed,Now).
	Now time.Time
}

// Snapshot creates a SnapshotTick from the receiver.
func (t HistogramTick) Snapshot() SnapshotTick {
	return SnapshotTick{
		Name:    t.Name,
		Elapsed: t.Elapsed,
		Now:     t.Now,
		Hist:    t.Hist.Export(),
	}
}

// SnapshotTick parallels HistogramTick but replace the histogram with a
// snapshot that is suitable for serialization. Additionally, it only contains
// the per-tick histogram, not the cumulative histogram. (The cumulative
// histogram can be computed by aggregating all of the per-tick histograms).
type SnapshotTick struct {
	Name    string
	Hist    *hdrhistogram.Snapshot
	Elapsed time.Duration
	Now     time.Time
}
