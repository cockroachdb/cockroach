// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// ExecutorMetrics describes metrics related to scheduled
// job executor operations.
type ExecutorMetrics struct {
	NumStarted   *metric.Counter
	NumSucceeded *metric.Counter
	NumFailed    *metric.Counter
}

var _ metric.Struct = &ExecutorMetrics{}

// MetricStruct implements metric.Struct interface
func (m *ExecutorMetrics) MetricStruct() {}

// SchedulerMetrics are metrics specific to job scheduler daemon.
type SchedulerMetrics struct {
	// Number of schedules that were ready to execute.
	ReadyToRun *metric.Gauge
	// Number of scheduled jobs started.
	NumStarted *metric.Gauge
	// Number of jobs started by schedules that are currently running.
	NumRunning *metric.Gauge
	// Number of schedules rescheduled due to SKIP policy.
	RescheduleSkip *metric.Gauge
	// Number of schedules rescheduled due to WAIT policy.
	RescheduleWait *metric.Gauge
	// Number of schedules that could not be processed due to an error.
	NumErrSchedules *metric.Gauge
	// Number of schedules that are malformed: that is, the schedules
	// we cannot parse, or even attempt to execute.
	NumMalformedSchedules *metric.Gauge
}

// MakeSchedulerMetrics returns metrics for scheduled job daemon.
func MakeSchedulerMetrics() SchedulerMetrics {
	return SchedulerMetrics{
		ReadyToRun: metric.NewGauge(metric.Metadata{
			Name:        "schedules.round.schedules-ready-to-run",
			Help:        "The number of jobs ready to execute",
			Measurement: "Schedules",
			Unit:        metric.Unit_COUNT,
		}),

		NumRunning: metric.NewGauge(metric.Metadata{
			Name:        "schedules.round.num-jobs-running",
			Help:        "The number of jobs started by schedules that are currently running",
			Measurement: "Jobs",
			Unit:        metric.Unit_COUNT,
		}),

		NumStarted: metric.NewGauge(metric.Metadata{
			Name:        "schedules.round.jobs-started",
			Help:        "The number of jobs started",
			Measurement: "Jobs",
			Unit:        metric.Unit_COUNT,
		}),

		RescheduleSkip: metric.NewGauge(metric.Metadata{
			Name:        "schedules.round.reschedule-skip",
			Help:        "The number of schedules rescheduled due to SKIP policy",
			Measurement: "Schedules",
			Unit:        metric.Unit_COUNT,
		}),

		RescheduleWait: metric.NewGauge(metric.Metadata{
			Name:        "schedules.round.reschedule-wait",
			Help:        "The number of schedules rescheduled due to WAIT policy",
			Measurement: "Schedules",
			Unit:        metric.Unit_COUNT,
		}),

		NumErrSchedules: metric.NewGauge(metric.Metadata{
			Name:        "schedules.error",
			Help:        "Number of schedules which did not execute successfully",
			Measurement: "Schedules",
			Unit:        metric.Unit_COUNT,
		}),

		NumMalformedSchedules: metric.NewGauge(metric.Metadata{
			Name:        "schedules.malformed",
			Help:        "Number of malformed schedules",
			Measurement: "Schedules",
			Unit:        metric.Unit_COUNT,
		}),
	}
}

// MetricStruct implements metric.Struct interface
func (m *SchedulerMetrics) MetricStruct() {}

var _ metric.Struct = &SchedulerMetrics{}

// MakeExecutorMetrics creates metrics for scheduled job executor.
func MakeExecutorMetrics(name string) ExecutorMetrics {
	return ExecutorMetrics{
		NumStarted: metric.NewCounter(metric.Metadata{
			Name:        fmt.Sprintf("schedules.%s.started", name),
			Help:        fmt.Sprintf("Number of %s jobs started", name),
			Measurement: "Jobs",
			Unit:        metric.Unit_COUNT,
		}),

		NumSucceeded: metric.NewCounter(metric.Metadata{
			Name:        fmt.Sprintf("schedules.%s.succeeded", name),
			Help:        fmt.Sprintf("Number of %s jobs succeeded", name),
			Measurement: "Jobs",
			Unit:        metric.Unit_COUNT,
		}),

		NumFailed: metric.NewCounter(metric.Metadata{
			Name:        fmt.Sprintf("schedules.%s.failed", name),
			Help:        fmt.Sprintf("Number of %s jobs failed", name),
			Measurement: "Jobs",
			Unit:        metric.Unit_COUNT,
		}),
	}
}
