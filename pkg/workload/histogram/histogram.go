// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// TODO(tbg): rename this package to "workloadmetrics" or something like that.

package histogram

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/codahale/hdrhistogram"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	// PrometheusNamespace is the value that should be used for the
	// Namespace field whenever a workload-related prometheus metric
	// is defined.
	PrometheusNamespace = "workload"
	// MockWorkloadName is to be used in the event that a Registry is required
	// outside of a workload.
	MockWorkloadName = "mock"

	sigFigs    = 1
	minLatency = 100 * time.Microsecond
)

// NamedHistogram is a named histogram for use in Operations. It is threadsafe
// but intended to be thread-local.
type NamedHistogram struct {
	name                string
	prometheusHistogram prometheus.Histogram
	mu                  struct {
		syncutil.Mutex
		current *hdrhistogram.Histogram
	}
}

// newNamedHistogramLocked creates a new instance of a NamedHistogram for the
// given name and adds it to the list of histograms tracked under this name. It
// also creates a prometheus histogram, but it is shared across all
// NamedHistograms for the same name (i.e. it is created when the respective
// name is first seen). This is because the sharding within a name is a perf
// optimization that doesn't apply to the prometheus histogram (plus everything
// gets more complex once we have to merge multiple prom histograms together
// for exposure via Gatherer()). See *Histograms for details.
func (w *Registry) newNamedHistogramLocked(name string) *NamedHistogram {
	hist := &NamedHistogram{
		name:                name,
		prometheusHistogram: w.getPrometheusHistogramLocked(name),
	}
	hist.mu.current = w.newHistogram()
	return hist
}

// Record saves a new datapoint and should be called once per logical operation.
func (w *NamedHistogram) Record(elapsed time.Duration) {
	w.prometheusHistogram.Observe(float64(elapsed.Nanoseconds()) / float64(time.Second))
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
	workloadName string // name of the workload reporting to this registry
	promReg      *prometheus.Registry
	mu           struct {
		syncutil.Mutex
		// maps histogram name to []{histogram created through handle 1, histogram created through handle 2, ...}
		registered map[string][]*NamedHistogram
		// maps histogram name to a single prometheus histogram shared by all
		// handles. These will be registered with promReg.
		prometheusHistograms map[string]prometheus.Histogram
	}

	start         time.Time
	cumulative    map[string]*hdrhistogram.Histogram
	prevTick      map[string]time.Time
	histogramPool *sync.Pool
}

// NewRegistry returns an initialized Registry.
// maxLat is the maximum time that queries are expected to take to execute
// which is needed to initialize the pool of histograms.
// newPrometheusHistogramFunc specifies how to generate a new
// prometheus.Histogram with a given name. Use MockNewPrometheusHistogram
// if prometheus logging is undesired.
func NewRegistry(maxLat time.Duration, workloadName string) *Registry {
	r := &Registry{
		workloadName: workloadName,
		start:        timeutil.Now(),
		cumulative:   make(map[string]*hdrhistogram.Histogram),
		prevTick:     make(map[string]time.Time),
		promReg:      prometheus.NewRegistry(),
		histogramPool: &sync.Pool{
			New: func() interface{} {
				return hdrhistogram.New(minLatency.Nanoseconds(), maxLat.Nanoseconds(), sigFigs)
			},
		},
	}
	r.mu.registered = make(map[string][]*NamedHistogram)
	r.mu.prometheusHistograms = make(map[string]prometheus.Histogram)
	return r
}

// Registerer returns a prometheus.Registerer.
func (w *Registry) Registerer() prometheus.Registerer {
	return w.promReg
}

// Gatherer returns a prometheus.Gatherer.
func (w *Registry) Gatherer() prometheus.Gatherer {
	return w.promReg
}

func (w *Registry) newHistogram() *hdrhistogram.Histogram {
	h := w.histogramPool.Get().(*hdrhistogram.Histogram)
	return h
}

var invalidPrometheusMetricRe = regexp.MustCompile(`[^a-zA-Z0-9:_]`)

func cleanPrometheusName(name string) string {
	return invalidPrometheusMetricRe.ReplaceAllString(name, "_")
}

func makePrometheusLatencyHistogramBuckets() []float64 {
	// This covers 0.5ms to 12 minutes at good resolution, using 150 buckets.
	return prometheus.ExponentialBuckets(0.0005, 1.1, 150)
}

func (w *Registry) getPrometheusHistogramLocked(name string) prometheus.Histogram {
	ph, ok := w.mu.prometheusHistograms[name]

	if !ok {
		// Metric names must be sanitized or NewHistogram will panic.
		promName := cleanPrometheusName(name) + "_duration_seconds"
		ph = promauto.With(w.promReg).NewHistogram(prometheus.HistogramOpts{
			Namespace: PrometheusNamespace,
			Subsystem: cleanPrometheusName(w.workloadName),
			Name:      promName,
			Buckets:   makePrometheusLatencyHistogramBuckets(),
		})
		w.mu.prometheusHistograms[name] = ph
	}

	return ph
}

// GetHandle returns a thread-local handle for creating and registering
// NamedHistograms. A handle should be created for each long-lived goroutine
// for best performance, see the comment on Histograms.
func (w *Registry) GetHandle() *Histograms {
	hists := &Histograms{
		reg: w,
	}
	hists.mu.hists = make(map[string]*NamedHistogram)
	return hists
}

// Tick aggregates all registered histograms, grouped by name. It is expected to
// be called periodically from one goroutine. The closure must not leak references
// to the histograms contained in the Tick as their backing memory is pooled.
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
// NamedHistograms. A Histograms handle reduces mutex contention
// in the common case of many worker goroutines reporting under the
// same histogram name. It does so by collecting observations locally
// (and thus avoiding contention that would arise from all workers
// observing into the same histogram). When the parent Registry's Tick
// method is called, all Histograms handles for the same name will be
// visited and the observations merged.
//
// Note that there is also a cumulative shared prometheus histogram (exposed
// under Registry.Gatherer) but since its implementation already optimizes for
// concurrent access, a single instance per name is shared across all handles on
// a Registry.
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
	defer w.mu.Unlock()
	w.reg.mu.Lock()
	defer w.reg.mu.Unlock()

	hist, ok = w.mu.hists[name]
	if !ok {
		hist = w.reg.newNamedHistogramLocked(name)
		w.mu.hists[name] = hist
		w.reg.mu.registered[name] = append(w.reg.mu.registered[name], hist)
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
