// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package benchstat

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/perf/internal/stats"
	"golang.org/x/perf/storage/benchfmt"
)

// A Collection is a collection of benchmark results.
type Collection struct {
	// Configs, Groups, and Units give the set of configs,
	// groups, and units from the keys in Stats in an order
	// meant to match the order the benchmarks were read in.
	Configs, Groups, Units []string

	// Benchmarks gives the set of benchmarks from the keys in
	// Stats by group in an order meant to match the order
	// benchmarks were read in.
	Benchmarks map[string][]string

	// Metrics holds the accumulated metrics for each key.
	Metrics map[Key]*Metrics

	// DeltaTest is the test to use to decide if a change is significant.
	// If nil, it defaults to UTest.
	DeltaTest DeltaTest

	// Alpha is the p-value cutoff to report a change as significant.
	// If zero, it defaults to 0.05.
	Alpha float64

	// AddGeoMean specifies whether to add a line to the table
	// showing the geometric mean of all the benchmark results.
	AddGeoMean bool

	// SplitBy specifies the labels to split results by.
	// By default, results will only be split by full name.
	SplitBy []string

	// Order specifies the row display order for this table.
	// If Order is nil, the table rows are printed in order of
	// first appearance in the input.
	Order Order
}

// A Key identifies one metric (e.g., "ns/op", "B/op") from one
// benchmark (function name sans "Benchmark" prefix) and optional
// group in one configuration (input file name).
type Key struct {
	Config, Group, Benchmark, Unit string
}

// A Metrics holds the measurements of a single metric
// (for example, ns/op or MB/s)
// for all runs of a particular benchmark.
type Metrics struct {
	Unit    string    // unit being measured
	Values  []float64 // measured values
	RValues []float64 // Values with outliers removed
	Min     float64   // min of RValues
	Mean    float64   // mean of RValues
	Max     float64   // max of RValues
}

// FormatMean formats m.Mean using scaler.
func (m *Metrics) FormatMean(scaler Scaler) string {
	var s string
	if scaler != nil {
		s = scaler(m.Mean)
	} else {
		s = fmt.Sprint(m.Mean)
	}
	return s
}

// FormatDiff computes and formats the percent variation of max and min compared to mean.
// If b.Mean or b.Max is zero, FormatDiff returns an empty string.
func (m *Metrics) FormatDiff() string {
	if m.Mean == 0 || m.Max == 0 {
		return ""
	}
	diff := 1 - m.Min/m.Mean
	if d := m.Max/m.Mean - 1; d > diff {
		diff = d
	}
	return fmt.Sprintf("%.0f%%", diff*100.0)
}

// Format returns a textual formatting of "Mean ±Diff" using scaler.
func (m *Metrics) Format(scaler Scaler) string {
	if m.Unit == "" {
		return ""
	}
	mean := m.FormatMean(scaler)
	diff := m.FormatDiff()
	if diff == "" {
		return mean + "     "
	}
	return fmt.Sprintf("%s ±%3s", mean, diff)
}

// computeStats updates the derived statistics in m from the raw
// samples in m.Values.
func (m *Metrics) computeStats() {
	// Discard outliers.
	values := stats.Sample{Xs: m.Values}
	q1, q3 := values.Percentile(0.25), values.Percentile(0.75)
	lo, hi := q1-1.5*(q3-q1), q3+1.5*(q3-q1)
	for _, value := range m.Values {
		if lo <= value && value <= hi {
			m.RValues = append(m.RValues, value)
		}
	}

	// Compute statistics of remaining data.
	m.Min, m.Max = stats.Bounds(m.RValues)
	m.Mean = stats.Mean(m.RValues)
}

// addMetrics returns the metrics with the given key from c,
// creating a new one if needed.
func (c *Collection) addMetrics(key Key) *Metrics {
	if c.Metrics == nil {
		c.Metrics = make(map[Key]*Metrics)
	}
	if stat, ok := c.Metrics[key]; ok {
		return stat
	}

	addString := func(strings *[]string, add string) {
		for _, s := range *strings {
			if s == add {
				return
			}
		}
		*strings = append(*strings, add)
	}
	addString(&c.Configs, key.Config)
	addString(&c.Groups, key.Group)
	if c.Benchmarks == nil {
		c.Benchmarks = make(map[string][]string)
	}
	benchmarks := c.Benchmarks[key.Group]
	addString(&benchmarks, key.Benchmark)
	c.Benchmarks[key.Group] = benchmarks
	addString(&c.Units, key.Unit)
	m := &Metrics{Unit: key.Unit}
	c.Metrics[key] = m
	return m
}

// AddConfig adds the benchmark results in the formatted data
// to the named configuration.
func (c *Collection) AddConfig(config string, data []byte) {
	c.Configs = append(c.Configs, config)
	key := Key{Config: config}
	br := benchfmt.NewReader(bytes.NewReader(data))
	for br.Next() {
		c.addResult(key, br.Result())
	}
	if err := br.Err(); err != nil {
		// bytes.Reader never returns errors
		panic(err)
	}
}

// AddResults adds the benchmark results to the named configuration.
func (c *Collection) AddResults(config string, results []*benchfmt.Result) {
	c.Configs = append(c.Configs, config)
	key := Key{Config: config}
	for _, r := range results {
		c.addResult(key, r)
	}
}

func (c *Collection) addResult(key Key, r *benchfmt.Result) {
	f := strings.Fields(r.Content)
	if len(f) < 4 {
		return
	}
	name := f[0]
	if !strings.HasPrefix(name, "Benchmark") {
		return
	}
	name = strings.TrimPrefix(name, "Benchmark")
	n, _ := strconv.Atoi(f[1])
	if n == 0 {
		return
	}
	key.Group = c.makeGroup(r)
	key.Benchmark = name
	for i := 2; i+2 <= len(f); i += 2 {
		val, err := strconv.ParseFloat(f[i], 64)
		if err != nil {
			continue
		}
		key.Unit = f[i+1]
		m := c.addMetrics(key)
		m.Values = append(m.Values, val)
	}
}

func (c *Collection) makeGroup(r *benchfmt.Result) string {
	var out string
	for _, s := range c.SplitBy {
		v := r.NameLabels[s]
		if v == "" {
			v = r.Labels[s]
		}
		if v != "" {
			if out != "" {
				out = out + " "
			}
			out += fmt.Sprintf("%s:%s", s, v)
		}
	}
	return out
}
