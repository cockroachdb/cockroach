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

var scale5S = timeScale{"5s", 5 * time.Second}
var scale1M = timeScale{"1m", 1 * time.Minute}
var scale5M = timeScale{"5m", 5 * time.Minute}
var scale30M = timeScale{"30m", 30 * time.Minute}
var scale1H = timeScale{"1h", time.Hour}
var scale1D = timeScale{"1d", 24 * time.Hour}

// Iterable provides a method for synchronized access to interior objects.
type Iterable interface {
	// Each calls the given closure with each contained item. The closure must
	// copy values it plans to use after returning.
	Each(func(string, interface{}))
}

var _ Iterable = &Gauge{}
var _ Iterable = &Counter{}
var _ Iterable = &Histogram{}
var _ Iterable = &EWMA{}

// Registry contains methods to collect metrics.
type Registry interface {
	Iterable
	Add(string, Iterable)
}

type periodic interface {
	tickInterval() time.Duration
	tickFn()
}

var _ periodic = &Histogram{}
var _ periodic = &EWMA{}

var _ Registry = &registry{}

// A registry bundles up various registries to provide a single point of
// access to them.
type registry struct {
	sync.Mutex
	tracked map[string]Iterable
	closer  <-chan struct{}
}

// NewRegistry creates a new Registry. Some types of metrics require an associated
// goroutine to ping them at fixed intervals; these goroutines will select on the
// supplied closer.
func NewRegistry(closer <-chan struct{}) Registry {
	return &registry{
		tracked: map[string]Iterable{},
		closer:  closer,
	}
}

func periodicAction(pi periodic, closer <-chan struct{}) {
	d := pi.tickInterval()
	if d <= 0 {
		return
	}
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-closer:
			return
		case <-t.C:
			pi.tickFn()
		}
	}
}

// Add links the given registry into this registry using the given format
// string. The individual items in the registry will be formatted via
// fmt.Sprintf(format, <name>). As a special case, metrics itself also implement
// Iterable and can thus be added to a registry.
func (r *registry) Add(format string, item Iterable) {
	r.Lock()
	r.tracked[format] = item
	if pi, ok := item.(periodic); ok {
		// TODO(tschottdorf): lots of goroutines to be saved here in exchange
		// for a little complexity.
		go periodicAction(pi, r.closer)
	}
	r.Unlock()
}

// Each calls the given closure for all metrics.
func (r *registry) Each(f func(name string, val interface{})) {
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
func (r *registry) MarshalJSON() ([]byte, error) {
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

// A Histogram is a wrapper around an hdrhistogram.WindowedHistogram.
type Histogram struct {
	unit     int64
	interval time.Duration
	maxVal   int64

	mu       sync.Mutex
	windowed *hdrhistogram.WindowedHistogram
}

func (h *Histogram) tickInterval() time.Duration {
	return h.interval
}

func (h *Histogram) tickFn() {
	h.mu.Lock()
	h.windowed.Rotate()
	h.mu.Unlock()
}

// RegisterHistogram registers a new windowed HDRHistogram with the given
// parameters. Data is kept in the active window for approximately the given
// duration.
func RegisterHistogram(name string, duration time.Duration, unit Unit, maxVal MaxVal, sigFigs int, r Registry) *Histogram {
	const n = 4
	h := &Histogram{}
	h.maxVal = int64(maxVal)
	h.unit = int64(unit)
	h.interval = duration / n

	h.windowed = hdrhistogram.NewWindowed(n, 0, h.maxVal/h.unit, sigFigs)
	r.Add(name, h)
	return h
}

// RegisterLatency is a convenience function which registers histograms with
// suitable defaults for latency tracking on millisecond to minute time scales.
// The generated names of the metric can be controlled via the given format
// string.
func RegisterLatency(format string, r Registry) Histograms {
	windows := []timeScale{scale5M, scale1H, scale1D}
	hs := make([]*Histogram, 0, 3)
	for _, w := range windows {
		h := RegisterHistogram(fmt.Sprintf(format, w.name), w.d, UnitMs, MaxMinute, 2, r)
		hs = append(hs, h)
	}
	return hs
}

// MarshalJSON outputs to JSON.
func (h *Histogram) MarshalJSON() ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return json.Marshal(h.windowed.Merge().CumulativeDistribution())
}

// RecordValue adds the given value to the histogram, truncating if necessary.
func (h *Histogram) RecordValue(v int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	v /= h.unit
	for h.windowed.Current.RecordValue(v) != nil {
		v = h.maxVal / h.unit
	}
}

// Each calls the closure with the empty string and the (locked) receiver.
func (h *Histogram) Each(f func(string, interface{})) {
	h.mu.Lock()
	defer h.mu.Unlock()
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

// RegisterCounter registers a new counter under the given name.
func RegisterCounter(name string, r Registry) *Counter {
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

// RegisterGauge registers a new Gauge with the given name.
func RegisterGauge(name string, r Registry) *Gauge {
	g := &Gauge{metrics.NewGauge()}
	r.Add(name, g)
	return g
}

// An EWMA is a exponential weighted moving average.
type EWMA struct {
	curSum   float64
	interval time.Duration

	mu      sync.Mutex
	wrapped ewma.MovingAverage
}

// RegisterEWMA registers an EWMA over the given timescale. Timescales at or
// below 2s are illegal and will cause a panic.
func RegisterEWMA(name string, timescale time.Duration, r Registry) *EWMA {
	const tickInterval = time.Second
	if timescale <= 2*time.Second {
		panic(fmt.Sprintf("EWMA with per-second ticks makes no sense on timescale %s", timescale))
	}
	avgAge := float64(timescale) / float64(2*tickInterval)

	e := &EWMA{
		interval: tickInterval,
		wrapped:  ewma.NewMovingAverage(avgAge),
	}
	r.Add(name, e)
	return e
}

func (e *EWMA) tickInterval() time.Duration {
	return e.interval
}

func (e *EWMA) tickFn() {
	e.mu.Lock()
	e.wrapped.Add(e.curSum)
	e.curSum = 0
	e.mu.Unlock()
}

// Add adds the given measurement to the EWMA.
func (e *EWMA) Add(v float64) {
	e.mu.Lock()
	e.curSum += v
	e.mu.Unlock()
}

// Each calls the given closure with the empty string and the EWMA.
func (e *EWMA) Each(f func(string, interface{})) {
	e.mu.Lock()
	defer e.mu.Unlock()
	f("", e.wrapped.Value())
}

// MarshalJSON marshals to JSON.
func (e *EWMA) MarshalJSON() ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return json.Marshal(e.wrapped.Value())
}

// EWMAS is a slice of EWMA metrics.
type EWMAS []*EWMA

// RegisterEWMAS returns a slice of EWMAs with the given format string and
// various "standard" timescales.
func RegisterEWMAS(format string, r Registry) EWMAS {
	scales := []timeScale{scale5S, scale1M, scale30M, scale1H}
	es := make([]*EWMA, 0, len(scales))
	for _, scale := range scales {
		es = append(es, RegisterEWMA(fmt.Sprintf(format, scale.name),
			scale.d, r))
	}
	return es
}

// Add adds the given value to all underlying EWMAs.
func (es EWMAS) Add(v float64) {
	for _, e := range es {
		e.Add(v)
	}
}
