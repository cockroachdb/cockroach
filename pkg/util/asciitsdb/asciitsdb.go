// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package asciitsdb

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/guptarohit/asciigraph"
	"github.com/stretchr/testify/require"
)

// TSDB is used to plot ASCII timeseries for tests that want to observe how CRDB
// metrics behave.
type TSDB struct {
	t   *testing.T
	reg *metric.Registry

	mu struct {
		syncutil.Mutex
		scraped bool
		points  map[string][]float64 // registered metric name => scraped data points
	}
}

// New returns a new ASCII TSDB.
func New(t *testing.T, reg *metric.Registry) *TSDB {
	tsdb := &TSDB{t: t, reg: reg}
	tsdb.mu.points = make(map[string][]float64)
	return tsdb
}

// Register registers a metric struct for plotting. For histograms, we follow
// CRDB's internal TSDB naming conventions, i.e. we export values for:
// - {metric name}-count
// - {metric name}-avg
// - {metric name}-max
// - {metric name}-p99
// - {metric name}-p90
// - {metric name}-p75
// - {metric name}-p50
func (t *TSDB) Register(mstruct interface{}) {
	// NB: This code was cargo culted from the metric registry's
	// AddStruct method.

	v := reflect.ValueOf(mstruct)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	typ := v.Type()

	for i := 0; i < v.NumField(); i++ {
		vfield, tfield := v.Field(i), typ.Field(i)
		tname := tfield.Name
		if !vfield.CanInterface() {
			t.t.Logf("skipping unexported field %s", tname)
			continue
		}
		switch vfield.Kind() {
		case reflect.Array:
			for i := 0; i < vfield.Len(); i++ {
				velem := vfield.Index(i)
				telemName := fmt.Sprintf("%s[%d]", tname, i)
				// Permit elements in the array to be nil.
				const skipNil = true
				t.registerMetricValue(velem, telemName, skipNil)
			}
		default:
			// No metric fields should be nil.
			const skipNil = false
			t.registerMetricValue(vfield, tname, skipNil)
		}
	}
}

// Scrape all registered metrics. It records a single data point for all
// registered metrics.
func (t *TSDB) Scrape(ctx context.Context) {
	// TB: This code is cargo culted entirely from the TSDB scraper.

	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.scraped = true

	t.reg.Each(func(name string, val interface{}) {
		if _, ok := t.mu.points[name]; !ok {
			return
		}

		switch mtr := val.(type) {
		case metric.WindowedHistogram:
			if _, ok := t.mu.points[name]; !ok {
				return
			}
			// Use cumulative stats here. Count must be calculated against the cumulative histogram.
			cumulative, ok := mtr.(metric.CumulativeHistogram)
			if !ok {
				panic(errors.AssertionFailedf(`extractValue called on histogram metric %q that does not implement the
				CumulativeHistogram interface. All histogram metrics are expected to implement this interface`, name))
			}
			count, _ := cumulative.CumulativeSnapshot().Total()
			t.mu.points[name+"-count"] = append(t.mu.points[name+"-count"], float64(count))
			// Use windowed stats for avg and quantiles
			windowedSnapshot := mtr.WindowedSnapshot()
			avg := windowedSnapshot.Mean()
			if math.IsNaN(avg) || math.IsInf(avg, +1) || math.IsInf(avg, -1) {
				avg = 0
			}
			t.mu.points[name+"-avg"] = append(t.mu.points[name+"-avg"], avg)
			for _, pt := range []quantile{
				{"-max", 100},
				{"-p99", 99},
				{"-p90", 90},
				{"-p75", 75},
				{"-p50", 50},
			} {
				t.mu.points[name+pt.suffix] = append(t.mu.points[name+pt.suffix], windowedSnapshot.ValueAtQuantile(pt.quantile))
			}
		case metric.PrometheusExportable:
			// NB: this branch is intentionally at the bottom since all metrics
			// implement it.
			m := mtr.ToPrometheusMetric()
			if m.Gauge != nil {
				t.mu.points[name] = append(t.mu.points[name], *m.Gauge.Value)
			} else if m.Counter != nil {
				t.mu.points[name] = append(t.mu.points[name], *m.Counter.Value)
			}
		default:
			log.Fatalf(ctx, "cannot extract value for type %T", mtr)
		}
	})
}

// Plot plots the given metrics, using the given options.
func (t *TSDB) Plot(metrics []string, options ...Option) string {
	c := configure(config{
		limit: math.MaxInt,
	}, options)

	var plots [][]float64
	for _, metric := range metrics {
		points, ok := t.read(metric)
		require.Truef(t.t, ok, "%s not found", metric)
		if c.rate != 0 {
			points = rate(points, c.rate)
		}
		if c.divisor != 0 {
			points = divide(points, c.divisor)
		}
		c.limit = min(c.limit, int64(len(points))-c.offset)
		points = points[c.offset : c.offset+c.limit]
		plots = append(plots, points)
	}
	return asciigraph.PlotMany(plots, c.graphopts...)
}

// Clear all recorded data points.
func (t *TSDB) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for metric := range t.mu.points {
		t.mu.points[metric] = nil
	}
}

// read reads all data points for the given metric name.
func (t *TSDB) read(metric string) ([]float64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	points, ok := t.mu.points[metric]
	return points, ok
}

// RegisteredMetricNames returns a list of all metric names that have been
// registerd with TSDB via Register(..).
func (t *TSDB) RegisteredMetricNames() []string {
	var names []string
	t.mu.Lock()
	defer t.mu.Unlock()

	for metric := range t.mu.points {
		names = append(names, metric)
	}

	// For deterministic output.
	sort.Strings(names)

	return names
}

func (t *TSDB) registerMetricValue(val reflect.Value, name string, skipNil bool) {
	if val.Kind() == reflect.Ptr && val.IsNil() {
		if skipNil {
			t.t.Logf("skipping nil metric field %s", name)
		} else {
			t.t.Fatalf("found nil metric field %s", name)
		}
		return
	}
	switch typ := val.Interface().(type) {
	case metric.Iterable:
		t.registerIterable(typ)
	case metric.Struct:
		t.Register(typ)
	default:
		t.t.Logf("skipping non-metric field %s", name)
	}
}

func (t *TSDB) registerIterable(metric metric.Iterable) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mu.scraped {
		t.t.Fatalf("register all metrics upfront before Scrape()")
	}
	t.mu.points[metric.GetName()] = []float64{}
}

// Option represents a configuration setting.
type Option interface {
	apply(c *config)
}

// WithRate plots the rate of growth for a given metric. The parameter is used
// to control how far back (with respect to individual points) compute the delta
// over.
func WithRate(r int) Option {
	return optionFunc(func(c *config) {
		c.rate = r
	})
}

// WithDivisor divides all individual points of a metric by the given divisor.
func WithDivisor(d float64) Option {
	return optionFunc(func(c *config) {
		c.divisor = d
	})
}

// WithGraphOptions configures the look and feel of the generated ASCII graphs.
func WithGraphOptions(opts ...asciigraph.Option) Option {
	return optionFunc(func(c *config) {
		c.graphopts = opts
	})
}

// WithOffset is used to offset a specified number of points in the graph.
func WithOffset(o int64) Option {
	return optionFunc(func(c *config) {
		c.offset = o
	})
}

// WithLimit is used to limit the number of points in the graph.
func WithLimit(l int64) Option {
	return optionFunc(func(c *config) {
		c.limit = l
	})
}

// config holds the various graphing options.
type config struct {
	rate          int
	divisor       float64
	offset, limit int64
	graphopts     []asciigraph.Option
}

// An optionFunc applies an option.
type optionFunc func(*config)

// apply implements the Option interface.
func (of optionFunc) apply(c *config) { of(c) }

func configure(defaults config, options []Option) *config {
	for _, o := range options {
		o.apply(&defaults)
	}
	return &defaults
}

// rate takes a list of individual data points (typically read from a cumulative
// counter) and returns a derivative, computed simply by taking each
// corresponding input point and subtracting one r points ago. If r points are
// collected per-{time unit}, this ends up computing the rate of growth
// per-{time unit}.
func rate(input []float64, r int) []float64 {
	rated := make([]float64, len(input))
	for i := 0; i < len(input); i++ {
		if i < r {
			rated[i] = 0
			continue
		}

		delta := input[i] - input[i-r]
		rated[i] = delta
	}
	return rated
}

// divide takes a list of individual data points and returns another, where each
// point is the corresponding input divided by the divisor.
func divide(input []float64, divisor float64) []float64 {
	output := make([]float64, len(input))
	for i := 0; i < len(input); i++ {
		output[i] = input[i] / divisor
	}
	return output
}

type quantile struct {
	suffix   string
	quantile float64
}
