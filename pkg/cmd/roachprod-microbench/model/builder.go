// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package model

import (
	"fmt"

	"golang.org/x/perf/benchfmt"
	"golang.org/x/perf/benchmath"
)

// metricUnitNames maps from unit to metric name for known built-in metrics.
var metricUnitNames = map[string]string{
	"ns/op": "time/op",
	"ns/GC": "time/GC",
	"B/op":  "alloc/op",
	"MB/s":  "speed",
}

// Builder is used to build a MetricMap from a set of benchmark results.
type Builder struct {
	metricMap  MetricMap
	thresholds *benchmath.Thresholds
	confidence float64
}

// NewBuilder creates a new builder.
func NewBuilder() *Builder {
	return &Builder{
		metricMap:  make(MetricMap),
		thresholds: &benchmath.DefaultThresholds,
		confidence: 0.95,
	}
}

// AddMetrics adds microbenchmark metrics to the builder's internal model by
// reading results from the given reader. The prefix is prepended to the
// benchmark name for all benchmarks. The id is used to identify the source of
// the metrics, e.g. the run the metrics came from.
func (b *Builder) AddMetrics(id, prefix string, reader *benchfmt.Reader) error {
	for reader.Scan() {
		switch rec := reader.Result().(type) {
		case *benchfmt.SyntaxError:
			// In case the benchmark log is corrupted or contains a syntax error, we
			// want to return an error to the caller.
			return fmt.Errorf("syntax error: %v", rec)
		case *benchfmt.Result:
			benchmarkName := prefix + string(rec.Name.Full())
			for _, value := range rec.Values {
				metric := b.resolveMetric(&value, reader.Units())
				entry := metric.resolveEntry(benchmarkName)
				entry.addValue(id, value.Value)
			}
		}
	}
	return nil
}

// ComputeMetricMap computes the samples and summaries for
// all metrics in the builder's model.
func (b *Builder) ComputeMetricMap() MetricMap {
	for _, metric := range b.metricMap {
		assumption := metric.Assumption
		for _, benchmarkEntry := range metric.BenchmarkEntries {
			benchmarkEntry.Samples = make(map[string]*benchmath.Sample)
			benchmarkEntry.Summaries = make(map[string]*benchmath.Summary)
			// Compute the samples and summaries for each run.
			for run, values := range benchmarkEntry.Values {
				samples := benchmath.NewSample(values, b.thresholds)
				benchmarkEntry.Samples[run] = samples
				summary := assumption.Summary(samples, b.confidence)
				benchmarkEntry.Summaries[run] = &summary
			}

		}
	}
	return b.metricMap
}

// ComputeComparison computes the Comparison between two runs of a specified
// benchmark, each run can contain multiple iterations. The oldID and newID are
// used to identify the source of the metrics, e.g. the runs the metrics came
// from. Iff either run does not exist, nil is returned.
func (m *Metric) ComputeComparison(benchmarkName, oldID, newID string) *Comparison {
	benchmarkEntry := m.BenchmarkEntries[benchmarkName]
	// Check that an entry for both runs exist. If not, return nil.
	for _, run := range []string{oldID, newID} {
		if benchmarkEntry.Samples[run] == nil || benchmarkEntry.Summaries[run] == nil {
			return nil
		}
	}
	// Compute the comparison and delta.
	comparison := Comparison{}
	oldSample, newSample := benchmarkEntry.Samples[oldID], benchmarkEntry.Samples[newID]
	comparison.Distribution = m.Assumption.Compare(oldSample, newSample)
	oldSummary, newSummary := benchmarkEntry.Summaries[oldID], benchmarkEntry.Summaries[newID]
	comparison.FormattedDelta = comparison.Distribution.FormatDelta(oldSummary.Center, newSummary.Center)
	if comparison.Distribution.P > comparison.Distribution.Alpha {
		comparison.Delta = 0.0
	} else {
		comparison.Delta = ((newSummary.Center / oldSummary.Center) - 1.0) * 100
	}
	return &comparison
}

// resolveMetric returns the Metric for the given value, creating it if it does
// not exist.
func (b *Builder) resolveMetric(value *benchfmt.Value, units benchfmt.UnitMetadataMap) *Metric {
	unit := value.Unit
	if metric, ok := b.metricMap[unit]; ok {
		return metric
	}
	metricName := metricUnitNames[unit]
	if metricName == "" {
		metricName = unit
	}
	better := units.GetBetter(unit)
	// If the unit is not known, assume that lower values are better (-1).
	if better == 0 {
		better = -1
	}
	metric := &Metric{
		Name:             metricName,
		Unit:             unit,
		Assumption:       units.GetAssumption(unit),
		Better:           better,
		BenchmarkEntries: make(map[string]*BenchmarkEntry),
	}
	b.metricMap[unit] = metric
	return metric
}

// resolveEntry returns the BenchmarkEntry for the given benchmark name, creating it if
// it does not exist.
func (m *Metric) resolveEntry(benchmarkName string) *BenchmarkEntry {
	if entry, ok := m.BenchmarkEntries[benchmarkName]; ok {
		return entry
	}
	entry := &BenchmarkEntry{
		Values: make(map[string][]float64),
	}
	m.BenchmarkEntries[benchmarkName] = entry
	return entry
}

// addValue adds the given value to the BenchmarkEntry for the given id.
func (e *BenchmarkEntry) addValue(id string, value float64) {
	e.Values[id] = append(e.Values[id], value)
}
