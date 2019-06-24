// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package histogram

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/codahale/hdrhistogram"
)

const (
	sigFigs    = 1
	minLatency = 100 * time.Microsecond
)

// NamedHistogram is a named histogram for use in Operations. It is threadsafe
// but intended to be thread-local.
type NamedHistogram struct {
	name string
	mu   struct {
		syncutil.Mutex
		current *hdrhistogram.Histogram
	}
}

func newNamedHistogram(reg *Registry, name string) *NamedHistogram {
	w := &NamedHistogram{name: name}
	w.mu.current = reg.newHistogram()
	return w
}

// Record saves a new datapoint and should be called once per logical operation.
func (w *NamedHistogram) Record(elapsed time.Duration) {
	maxLatency := time.Duration(w.mu.current.HighestTrackableValue())
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
func (w *NamedHistogram) tick(
	newHistogram *hdrhistogram.Histogram, fn func(h *hdrhistogram.Histogram),
) {
	w.mu.Lock()
	h := w.mu.current
	w.mu.current = newHistogram
	w.mu.Unlock()
	fn(h)
}

// Registry is a thread-safe enclosure for a (possibly large) number of
// named histograms. It allows for "tick"ing them periodically to reset the
// counts and also supports aggregations.
type Registry struct {
	mu struct {
		syncutil.Mutex
		registered map[string][]*NamedHistogram
	}

	start         time.Time
	cumulative    map[string]*hdrhistogram.Histogram
	prevTick      map[string]time.Time
	histogramPool *sync.Pool
}

// NewRegistry returns an initialized Registry. maxLat is the maximum time that
// queries are expected to take to execute which is needed to initialize the
// pool of histograms.
func NewRegistry(maxLat time.Duration) *Registry {
	r := &Registry{
		start:      timeutil.Now(),
		cumulative: make(map[string]*hdrhistogram.Histogram),
		prevTick:   make(map[string]time.Time),
		histogramPool: &sync.Pool{
			New: func() interface{} {
				return hdrhistogram.New(minLatency.Nanoseconds(), maxLat.Nanoseconds(), sigFigs)
			},
		},
	}
	r.mu.registered = make(map[string][]*NamedHistogram)
	return r
}

func (w *Registry) newHistogram() *hdrhistogram.Histogram {
	h := w.histogramPool.Get().(*hdrhistogram.Histogram)
	return h
}

// GetHandle returns a thread-local handle for creating and registering
// NamedHistograms.
func (w *Registry) GetHandle() *Histograms {
	hists := &Histograms{reg: w}
	hists.mu.hists = make(map[string]*NamedHistogram)
	return hists
}

// Tick aggregates all registered histograms, grouped by name. It is expected to
// be called periodically from one goroutine.
func (w *Registry) Tick(fn func(Tick)) {
	merged := make(map[string]*hdrhistogram.Histogram)
	var names []string
	var wg sync.WaitGroup

	w.mu.Lock()
	for name, nameRegistered := range w.mu.registered {
		wg.Add(1)
		registered := append([]*NamedHistogram(nil), nameRegistered...)
		merged[name] = w.newHistogram()
		names = append(names, name)
		go func(registered []*NamedHistogram, merged *hdrhistogram.Histogram) {
			for _, hist := range registered {
				hist.tick(w.newHistogram(), func(h *hdrhistogram.Histogram) {
					merged.Merge(h)
					h.Reset()
					w.histogramPool.Put(h)
				})
			}
			wg.Done()
		}(registered, merged[name])
	}
	w.mu.Unlock()

	wg.Wait()

	now := timeutil.Now()
	sort.Strings(names)
	for _, name := range names {
		mergedHist := merged[name]
		if _, ok := w.cumulative[name]; !ok {
			w.cumulative[name] = w.newHistogram()
		}
		w.cumulative[name].Merge(mergedHist)

		prevTick, ok := w.prevTick[name]
		if !ok {
			prevTick = w.start
		}
		w.prevTick[name] = now
		fn(Tick{
			Name:       name,
			Hist:       mergedHist,
			Cumulative: w.cumulative[name],
			Elapsed:    now.Sub(prevTick),
			Now:        now,
		})
		mergedHist.Reset()
		w.histogramPool.Put(mergedHist)
	}
}

// Histograms is a thread-local handle for creating and registering
// NamedHistograms.
type Histograms struct {
	reg *Registry
	mu  struct {
		syncutil.RWMutex
		hists map[string]*NamedHistogram
	}
}

// Get returns a NamedHistogram with the given name, creating and registering it
// if necessary. The result is cached, so no need to cache it in the workload.
func (w *Histograms) Get(name string) *NamedHistogram {
	// Fast path for existing histograms, which is the common case by far.
	w.mu.RLock()
	hist, ok := w.mu.hists[name]
	if ok {
		w.mu.RUnlock()
		return hist
	}
	w.mu.RUnlock()

	w.mu.Lock()
	hist, ok = w.mu.hists[name]
	if !ok {
		hist = newNamedHistogram(w.reg, name)
		w.mu.hists[name] = hist
	}
	w.mu.Unlock()

	if !ok {
		w.reg.mu.Lock()
		w.reg.mu.registered[name] = append(w.reg.mu.registered[name], hist)
		w.reg.mu.Unlock()
	}

	return hist
}

// Copy makes a new histogram which is a copy of h.
func Copy(h *hdrhistogram.Histogram) *hdrhistogram.Histogram {
	dup := hdrhistogram.New(h.LowestTrackableValue(), h.HighestTrackableValue(),
		int(h.SignificantFigures()))
	dup.Merge(h)
	return dup
}

// Tick is an aggregation of ticking all histograms in a
// Registry with a given name.
type Tick struct {
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
func (t Tick) Snapshot() SnapshotTick {
	return SnapshotTick{
		Name:    t.Name,
		Elapsed: t.Elapsed,
		Now:     t.Now,
		Hist:    t.Hist.Export(),
	}
}

// SnapshotTick parallels Tick but replace the histogram with a
// snapshot that is suitable for serialization. Additionally, it only contains
// the per-tick histogram, not the cumulative histogram. (The cumulative
// histogram can be computed by aggregating all of the per-tick histograms).
type SnapshotTick struct {
	Name    string
	Hist    *hdrhistogram.Snapshot
	Elapsed time.Duration
	Now     time.Time
}

// DecodeSnapshots decodes a file with SnapshotTicks into a series.
func DecodeSnapshots(path string) (map[string][]SnapshotTick, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	dec := json.NewDecoder(f)
	ret := make(map[string][]SnapshotTick)
	for {
		var tick SnapshotTick
		if err := dec.Decode(&tick); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		ret[tick.Name] = append(ret[tick.Name], tick)
	}
	return ret, nil
}
