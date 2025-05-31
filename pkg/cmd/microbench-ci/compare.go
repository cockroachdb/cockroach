// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"os"
	"path"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/model"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/perf/benchfmt"
	"golang.org/x/perf/benchmath"
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
	Improved
	Regressed
)

// String returns the string representation of the status.
func (s Status) String() string {
	switch s {
	case NoChange:
		return "No Change"
	case Improved:
		return "Improved"
	case Regressed:
		return "Regressed"
	default:
		panic(fmt.Sprintf("unknown status: %d", s))
	}
}

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
	if cc.Delta*float64(entry.Better) > 0 {
		status = Improved
	} else if cc.Delta*float64(entry.Better) < 0 {
		status = Regressed
	}

	// Check if the metric has a delta cutoff threshold.
	threshold := 0.0
	for _, metric := range c.Benchmark.Metrics {
		if metric.Name == metricName {
			threshold = metric.Threshold
			break
		}
	}
	// If the threshold is set and the delta is less than the threshold, we
	// consider the metric to have no change. This accounts for compiler induced
	// variance, where the regression might be reproducible, but the change is
	// unrelated to the changes in the code.
	if math.Abs(cc.Delta) < threshold {
		status = NoChange
	}
	return status
}

// compare compares the metrics of a benchmark between two revisions. Only the
// specified last number of lines of the benchmark logs are considered. If lines
// is 0, it considers the entire logs.
func (b *Benchmark) compare(lines int) (*CompareResult, error) {
	builder := model.NewBuilder(model.WithThresholds(&benchmath.Thresholds{
		CompareAlpha: b.CompareAlpha,
	}))
	compareResult := CompareResult{Benchmark: b}
	for _, revision := range []Revision{Old, New} {
		data, err := logTail(path.Join(suite.artifactsDir(revision), b.cleanLog()), lines)
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

// compareBenchmarks compares the metrics of all benchmarks between two
// revisions. It first compares only the last outer run of each benchmark. If
// the last run had significant changes, it compares the metrics of all runs.
// This is because the last run would only have completed with significant
// changes if all the previous runs had them as well, and then we want to
// include it in the final assessment. In contrast if the last run had no
// significant changes, it is possible that the previous runs had significant
// changes, and we don't want to include them in the final assessment.
func (b Benchmarks) compareBenchmarks() (CompareResults, error) {
	compareResults := make(CompareResults, 0, len(b))
	for _, benchmark := range b {
		compareResult, err := benchmark.compare(benchmark.Count)
		if err != nil {
			return nil, err
		}
		compareResults = append(compareResults, compareResult)
	}
	return compareResults, nil
}

// logTail returns the last N lines of a file.
// If N is 0, it returns the entire file.
func logTail(filePath string, N int) ([]byte, error) {
	if N == 0 {
		return os.ReadFile(filePath)
	}
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	lines := make([]string, 0, N)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > N {
			lines = lines[1:]
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	var buffer bytes.Buffer
	for _, line := range lines {
		buffer.WriteString(line + "\n")
	}
	return buffer.Bytes(), nil
}
