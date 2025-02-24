// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/model"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/perf/benchfmt"
)

type (
	CompareResult struct {
		Benchmark *Benchmark
		MetricMap model.MetricMap
		EntryName string
	}
	CompareResults []*CompareResult

	Status int
)

const (
	NoChange Status = iota
	Better
	Worse
	Regression
)

// status returns the status of a metric in the comparison.
func (c *CompareResult) status(metricName string) Status {
	entry := c.MetricMap[metricName]
	if entry == nil {
		return NoChange
	}
	cc := entry.ComputeComparison(c.EntryName, string(Old), string(New))
	if cc == nil {
		return NoChange
	}
	status := NoChange
	threshold := c.Benchmark.Thresholds[metricName] * 100.0
	if cc.Delta*float64(entry.Better) > 0 {
		status = Better
	} else if cc.Delta*float64(entry.Better) < 0 {
		status = Worse
		if math.Abs(cc.Delta) >= threshold {
			status = Regression
		}
	}
	return status
}

// regressed returns true if any metric in the comparison has regressed.
func (c *CompareResult) regressed() bool {
	for metric := range c.MetricMap {
		status := c.status(metric)
		if status == Regression {
			return true
		}
	}
	return false
}

// compare compares the metrics of a benchmark between two revisions.
func (b *Benchmark) compare() (*CompareResult, error) {
	builder := model.NewBuilder()
	compareResult := CompareResult{Benchmark: b}
	for _, revision := range []Revision{Old, New} {
		data, err := os.ReadFile(path.Join(suite.artifactsDir(revision), b.cleanLog()))
		if err != nil {
			return nil, err
		}
		reader := benchfmt.NewReader(bytes.NewReader(data), b.cleanLog())
		err = builder.AddMetrics(string(revision), "", reader)
		if err != nil {
			return nil, err
		}
	}

	metricMap := builder.ComputeMetricMap()
	if len(metricMap) == 0 {
		return nil, errors.Newf("no metrics found for benchmark %s", b.Name)
	}
	entry := metricMap[maps.Keys(metricMap)[0]]
	if len(entry.BenchmarkEntries) == 0 {
		return nil, errors.Newf("no benchmark entries found for benchmark %s", b.Name)
	}
	entries := len(maps.Keys(entry.BenchmarkEntries))
	if entries != 1 {
		hint := fmt.Sprintf("please ensure the benchmark name %q matches a single benchmark", b.Name)
		return nil, errors.Newf("unexpected multiple benchmark entries (%s): %v",
			hint, maps.Keys(entry.BenchmarkEntries))
	}

	compareResult.EntryName = maps.Keys(entry.BenchmarkEntries)[0]
	compareResult.MetricMap = metricMap
	return &compareResult, nil
}

// compareBenchmarks compares the metrics of all benchmarks between two revisions.
func (b Benchmarks) compareBenchmarks() (CompareResults, error) {
	compareResults := make(CompareResults, 0, len(b))
	for _, benchmark := range b {
		compareResult, err := benchmark.compare()
		if err != nil {
			return nil, err
		}
		compareResults = append(compareResults, compareResult)
	}
	return compareResults, nil
}
