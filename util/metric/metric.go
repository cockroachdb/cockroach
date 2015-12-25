package metric

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/codahale/hdrhistogram"
	"github.com/rcrowley/go-metrics"
)

type timeScale struct {
	name string
	d    time.Duration
}

var scale1M = timeScale{"1m", 1 * time.Minute}
var scale10M = timeScale{"10m", 10 * time.Minute}
var scale1H = timeScale{"1h", time.Hour}

// Iterable provides a method for synchronized access to interior objects.
type Iterable interface {
	// Each calls the given closure with each contained item. The closure must
	// copy values it plans to use after returning.
	Each(func(string, interface{}))
}

var _ Iterable = &Gauge{}
var _ Iterable = &Counter{}
var _ Iterable = &Histogram{}
var _ Iterable = &Rate{}

type periodic interface {
	nextTick() time.Time
	tick()
}

var _ periodic = &Histogram{}
var _ periodic = &Rate{}

// A Registry bundles up various iterables (i.e. typically metrics or other
// registries) to provide a single point of access to them.
type Registry struct {
	sync.Mutex
	tracked map[string]Iterable
	closer  <-chan struct{}
}

// NewRegistry creates a new Registry. Some types of metrics require an associated
// goroutine to ping them at fixed intervals; these goroutines will select on the
// supplied closer.
func NewRegistry(closer <-chan struct{}) *Registry {
	return &Registry{
		tracked: map[string]Iterable{},
		closer:  closer,
	}
}

// Add links the given Iterable into this registry using the given format
// string. The individual items in the registry will be formatted via
// fmt.Sprintf(format, <name>). As a special case, metrics itself also implement
// Iterable and can thus be added to a registry.
func (r *Registry) Add(format string, item Iterable) {
	r.Lock()
	r.tracked[format] = item
	r.Unlock()
}

// Each calls the given closure for all metrics.
func (r *Registry) Each(f func(name string, val interface{})) {
	r.Lock()
	defer r.Unlock()
	for format, registry := range r.tracked {
		registry.Each(func(name string, v interface{}) {
			if name == "" {
				f(format, v)
			} else {
				f(fmt.Sprintf(format, name), v)
			}
		})
	}
}

// MarshalJSON marshals to JSON.
func (r *Registry) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})
	r.Each(func(name string, v interface{}) {
		m[name] = v
	})
	return json.Marshal(m)
}

// Unit is a base unit for a histogram.
type Unit int64

// UnitMs is the number of milliseconds in a nanosecond.
const UnitMs = Unit(time.Millisecond)

// MaxVal is a maximum value for a histogram.
type MaxVal int64

// MaxMinute truncates histogram values larger than one minute.
const MaxMinute = MaxVal(time.Minute)

func maybeTick(m periodic) {
	for m.nextTick().Before(time.Now()) {
		m.tick()
	}
}

// A Histogram is a wrapper around an hdrhistogram.WindowedHistogram.
type Histogram struct {
	unit   int64
	maxVal int64

	mu       sync.Mutex
	windowed *hdrhistogram.WindowedHistogram
	interval time.Duration
	nextT    time.Time
}

func (h *Histogram) tick() {
	h.nextT = h.nextT.Add(h.interval)
	h.windowed.Rotate()
}

func (h *Histogram) nextTick() time.Time {
	return h.nextT
}

// Histogram registers a new windowed HDRHistogram with the given
// parameters. Data is kept in the active window for approximately the given
// duration.
func (r *Registry) Histogram(name string, duration time.Duration, unit Unit, maxVal MaxVal, sigFigs int) *Histogram {
	const n = 4
	h := &Histogram{}
	h.maxVal = int64(maxVal)
	h.unit = int64(unit)
	h.interval = duration / n
	h.nextT = time.Now()

	h.windowed = hdrhistogram.NewWindowed(n, 0, h.maxVal/h.unit, sigFigs)
	r.Add(name, h)
	return h
}

// Latency is a convenience function which registers histograms with
// suitable defaults for latency tracking on millisecond to minute time scales.
// The generated names of the metric can be controlled via the given format
// string.
func (r *Registry) Latency(format string) Histograms {
	windows := []timeScale{scale1M, scale10M, scale1H}
	hs := make([]*Histogram, 0, 3)
	for _, w := range windows {
		h := r.Histogram(fmt.Sprintf(format, w.name), w.d, UnitMs, MaxMinute, 2)
		hs = append(hs, h)
	}
	return hs
}

// MarshalJSON outputs to JSON.
func (h *Histogram) MarshalJSON() ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	maybeTick(h)
	return json.Marshal(h.windowed.Merge().CumulativeDistribution())
}

// RecordValue adds the given value to the histogram, truncating if necessary.
func (h *Histogram) RecordValue(v int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	maybeTick(h)
	v /= h.unit
	for h.windowed.Current.RecordValue(v) != nil {
		v = h.maxVal / h.unit
	}
}

// Each calls the closure with the empty string and the (locked) receiver.
func (h *Histogram) Each(f func(string, interface{})) {
	h.mu.Lock()
	defer h.mu.Unlock()
	maybeTick(h)
	f("", h)
}

// Histograms is a slice of Histogram metrics.
type Histograms []*Histogram

// RecordValue calls through to each individual Histogram.
func (hs Histograms) RecordValue(v int64) {
	for _, h := range hs {
		h.RecordValue(v)
	}
}

// A Counter holds a single mutable atomic value.
type Counter struct {
	metrics.Counter
}

// Each calls the given closure with the empty string and itself.
func (c *Counter) Each(f func(string, interface{})) { f("", c) }

// MarshalJSON marshals to JSON.
func (c *Counter) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.Counter.Count())
}

// Counter registers a new counter under the given name.
func (r *Registry) Counter(name string) *Counter {
	c := &Counter{metrics.NewCounter()}
	r.Add(name, c)
	return c
}

// A Gauge atomically stores a single value.
type Gauge struct {
	metrics.Gauge
}

// Each calls the given closure with the empty string and itself.
func (g *Gauge) Each(f func(string, interface{})) { f("", g) }

// MarshalJSON marshals to JSON.
func (g *Gauge) MarshalJSON() ([]byte, error) {
	return json.Marshal(g.Gauge.Value())
}

// Gauge registers a new Gauge with the given name.
func (r *Registry) Gauge(name string) *Gauge {
	g := &Gauge{metrics.NewGauge()}
	r.Add(name, g)
	return g
}

// A Rate is a exponential weighted moving average.
type Rate struct {
	mu       sync.Mutex // protects fields below
	curSum   float64
	wrapped  ewma.MovingAverage
	interval time.Duration
	nextT    time.Time
}

// Rate registers an EWMA rate over the given timescale. Timescales at
// or below 2s are illegal and will cause a panic.
func (r *Registry) Rate(name string, timescale time.Duration) *Rate {
	const tickInterval = time.Second
	if timescale <= 2*time.Second {
		panic(fmt.Sprintf("EWMA with per-second ticks makes no sense on timescale %s", timescale))
	}
	avgAge := float64(timescale) / float64(2*tickInterval)

	e := &Rate{
		interval: tickInterval,
		nextT:    time.Now(),
		wrapped:  ewma.NewMovingAverage(avgAge),
	}
	r.Add(name, e)
	return e
}

func (e *Rate) nextTick() time.Time {
	return e.nextT
}

func (e *Rate) tick() {
	e.nextT = e.nextT.Add(e.interval)
	e.wrapped.Add(e.curSum)
	e.curSum = 0
}

// Add adds the given measurement to the Rate.
func (e *Rate) Add(v float64) {
	e.mu.Lock()
	maybeTick(e)
	e.curSum += v
	e.mu.Unlock()
}

// Each calls the given closure with the empty string and the Rate.
func (e *Rate) Each(f func(string, interface{})) {
	e.mu.Lock()
	defer e.mu.Unlock()
	maybeTick(e)
	f("", e.wrapped.Value())
}

// MarshalJSON marshals to JSON.
func (e *Rate) MarshalJSON() ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	maybeTick(e)
	return json.Marshal(e.wrapped.Value())
}

// Rates is a slice of EWMA backed rates.
type Rates []*Rate

// Rates returns a slice of EWMAs with the given format string and
// various "standard" timescales.
func (r *Registry) Rates(format string) Rates {
	scales := []timeScale{scale1M, scale10M, scale1H}
	es := make([]*Rate, 0, len(scales))
	for _, scale := range scales {
		es = append(es, r.Rate(fmt.Sprintf(format, scale.name),
			scale.d))
	}
	return es
}

// Add adds the given value to all underlying Rates.
func (es Rates) Add(v float64) {
	for _, e := range es {
		e.Add(v)
	}
}
