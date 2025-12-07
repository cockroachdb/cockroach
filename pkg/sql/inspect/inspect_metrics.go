// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// InspectMetrics holds metrics for INSPECT job operations.
type InspectMetrics struct {
	Runs           *metric.Counter
	RunsWithIssues *metric.Counter
	IssuesFound    *metric.Counter
	SpansProcessed *metric.Counter
}

var _ metric.Struct = (*InspectMetrics)(nil)

// MetricStruct implements the metric.Struct interface.
func (InspectMetrics) MetricStruct() {}

var (
	metaInspectRuns = metric.Metadata{
		Name:        "jobs.inspect.runs",
		Help:        "Number of INSPECT jobs executed",
		Measurement: "Jobs",
		Unit:        metric.Unit_COUNT,
	}
	metaInspectRunsWithIssues = metric.Metadata{
		Name:        "jobs.inspect.runs_with_issues",
		Help:        "Number of INSPECT jobs that found at least one issue",
		Measurement: "Jobs",
		Unit:        metric.Unit_COUNT,
	}
	metaInspectIssuesFound = metric.Metadata{
		Name:        "jobs.inspect.issues_found",
		Help:        "Total count of issues found by INSPECT jobs",
		Measurement: "Issues",
		Unit:        metric.Unit_COUNT,
	}
	metaInspectSpansProcessed = metric.Metadata{
		Name:        "jobs.inspect.spans_processed",
		Help:        "Number of spans processed by INSPECT jobs",
		Measurement: "Spans",
		Unit:        metric.Unit_COUNT,
	}
)

// MakeInspectMetrics instantiates the metrics for INSPECT jobs.
func MakeInspectMetrics(histogramWindow time.Duration) metric.Struct {
	return &InspectMetrics{
		Runs:           metric.NewCounter(metaInspectRuns),
		RunsWithIssues: metric.NewCounter(metaInspectRunsWithIssues),
		IssuesFound:    metric.NewCounter(metaInspectIssuesFound),
		SpansProcessed: metric.NewCounter(metaInspectSpansProcessed),
	}
}

func init() {
	jobs.MakeInspectMetricsHook = MakeInspectMetrics
}
