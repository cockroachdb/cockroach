package systembench

import (
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"sort"
	"sync"
	"time"

	"github.com/codahale/hdrhistogram"
)

const (
	minLatency = 10 * time.Microsecond
	maxLatency = 10 * time.Second
)

func newHistogram() *hdrhistogram.Histogram {
	return hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
}

type namedHistogram struct {
	name string
	mu   struct {
		sync.Mutex
		current *hdrhistogram.Histogram
	}
}

func newNamedHistogram(name string) *namedHistogram {
	w := &namedHistogram{name: name}
	w.mu.current = newHistogram()
	return w
}

func (w *namedHistogram) Record(elapsed time.Duration) {
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

func (w *namedHistogram) tick(fn func(h *hdrhistogram.Histogram)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	h := w.mu.current
	w.mu.current = newHistogram()
	fn(h)
}

type histogramTick struct {
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

type histogramRegistry struct {
	mu struct {
		sync.Mutex
		registered []*namedHistogram
	}

	start      time.Time
	cumulative map[string]*hdrhistogram.Histogram
	prevTick   map[string]time.Time
}

// NewHistogramRegistry returns an initialized HistogramRegistry.
func NewHistogramRegistry() *histogramRegistry {
	r := &histogramRegistry{
		start:      timeutil.Now(),
		cumulative: make(map[string]*hdrhistogram.Histogram),
		prevTick:   make(map[string]time.Time),
	}
	r.mu.registered = []*namedHistogram{}
	return r
}

func newHistogramRegistry() *histogramRegistry {
	return &histogramRegistry{
		start:      time.Now(),
		cumulative: make(map[string]*hdrhistogram.Histogram),
		prevTick:   make(map[string]time.Time),
	}
}

func (w *histogramRegistry) Register(name string) *namedHistogram {
	hist := newNamedHistogram(name)

	w.mu.Lock()
	w.mu.registered = append(w.mu.registered, hist)
	w.mu.Unlock()

	return hist
}

func (w *histogramRegistry) Tick(fn func(histogramTick)) {
	w.mu.Lock()
	registered := append([]*namedHistogram(nil), w.mu.registered...)
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

	now := time.Now()
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
		fn(histogramTick{
			Name:       name,
			Hist:       merged[name],
			Cumulative: w.cumulative[name],
			Elapsed:    now.Sub(prevTick),
			Now:        now,
		})
	}
}
