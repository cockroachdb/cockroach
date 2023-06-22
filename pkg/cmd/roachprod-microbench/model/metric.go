// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package model

import "golang.org/x/perf/benchmath"

// MetricMap is a map from metric name to metric.
type MetricMap map[string]*Metric

// Metric contains the details of a metric and all benchmark entries associated
// with it.
type Metric struct {
	// Name is the name of the metric (e.g. "time/op").
	// This is usually the same as the unit, but not always.
	Name string
	// Unit is the unit of the metric (e.g. "ns/op").
	Unit string
	// Assumption is the statistical assumption to make about distributions of
	// values in this metric.
	Assumption benchmath.Assumption
	// Better is the direction of improvement for this metric.
	Better int
	// BenchmarkEntries is a map from full benchmark name to BenchmarkEntry.
	BenchmarkEntries map[string]*BenchmarkEntry
}

// BenchmarkEntry contains the samples and summaries for microbenchmark(s).
// The samples and summaries are keyed by the id of the source of the
// microbenchmark(s), e.g. the run the microbenchmark(s) came from.
type BenchmarkEntry struct {
	Values    map[string][]float64
	Samples   map[string]*benchmath.Sample
	Summaries map[string]*benchmath.Summary
}

// Comparison contains the results of comparing two microbenchmarks.
type Comparison struct {
	Distribution   benchmath.Comparison
	Delta          float64
	FormattedDelta string
}
