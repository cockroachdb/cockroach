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
	numUnderlyingHistograms = 1
	sigFigs                 = 1
	// TODO(dan): It seems these constants could due with some tuning. In
	// #22605, it was suggested to make them 100 microseconds minimum, 60
	// seconds maximum, and 3 sigfigs. I'm intentionally waiting on this until
	// after we're quite sure that the workload versions of `kv` and `tpcc` are
	// producing the same results as the old tools.
	minLatency = 100 * time.Microsecond
	maxLatency = 10 * time.Second
)

// NamedHistogram is a named histogram for use in Operations. It is threadsafe
// but intended to be thread-local.
type NamedHistogram struct {
	name string
	mu   struct {
		syncutil.Mutex
		numOps int64
		hist   *hdrhistogram.WindowedHistogram
	}
}

func newNamedHistogram(name string) *NamedHistogram {
	w := &NamedHistogram{name: name}
	// TODO(dan): Does this really need to be a windowed histogram, given that
	// we're using a window size of 1? I'm intentionally waiting on this until
	// after we're quite sure that the workload versions of `kv` and `tpcc` are
	// producing the same results as the old tools.
	w.mu.hist = hdrhistogram.NewWindowed(
		numUnderlyingHistograms, minLatency.Nanoseconds(), maxLatency.Nanoseconds(), sigFigs)
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
	err := w.mu.hist.Current.RecordValue(elapsed.Nanoseconds())
	w.mu.numOps++
	w.mu.Unlock()

	if err != nil {
		panic(fmt.Sprintf(`%s: recording value: %s`, w.name, err))
	}
}

// tick resets the current histogram to a new "period". The old one's data
// should be saved via the closure argument.
func (w *NamedHistogram) tick(fn func(numOps int64, h *hdrhistogram.Histogram)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	m := w.mu.hist.Merge()
	w.mu.hist.Rotate()
	fn(w.mu.numOps, m)
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
	prevTick   map[string]HistogramTick
}

// NewHistogramRegistry returns an initialized HistogramRegistry.
func NewHistogramRegistry() *HistogramRegistry {
	return &HistogramRegistry{
		start:      timeutil.Now(),
		cumulative: make(map[string]*hdrhistogram.Histogram),
		prevTick:   make(map[string]HistogramTick),
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
	totalOps := make(map[string]int64)
	for _, hist := range registered {
		hist.tick(func(numOps int64, h *hdrhistogram.Histogram) {
			// TODO(dan): It really seems like we should be able to use
			// `h.TotalCount()` for the number of operations but for some reason
			// that doesn't line up in practice. Investigate. Merge returns the
			// number of samples that had to be dropped during the merge - we
			// ignore that value. Perhaps that is the discrepancy.
			totalOps[hist.name] += numOps
			if m, ok := merged[hist.name]; ok {
				m.Merge(h)
			} else {
				merged[hist.name] = h
				names = append(names, hist.name)
			}
		})
	}

	sort.Strings(names)
	for _, name := range names {
		mergedHist := merged[name]
		if _, ok := w.cumulative[name]; !ok {
			w.cumulative[name] = hdrhistogram.New(
				minLatency.Nanoseconds(), maxLatency.Nanoseconds(), sigFigs)
		}
		w.cumulative[name].Merge(mergedHist)

		prevTick, ok := w.prevTick[name]
		if !ok {
			prevTick.start = w.start
		}
		now := timeutil.Now()
		w.prevTick[name] = HistogramTick{
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
	Hist *hdrhistogram.Histogram
	// Cumulative is the merged result of the represented histograms for all
	// time.
	Cumulative *hdrhistogram.Histogram
	// Ops is the total number of `Record` calls for all represented histograms.
	Ops int64
	// LastOps is the value of Ops for the last tick.
	LastOps int64
	// Elapsed is the amount of time since the last tick.
	Elapsed time.Duration

	start time.Time
}
