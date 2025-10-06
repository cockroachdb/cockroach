// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// ExecutorMetrics describes metrics related to scheduled
// job executor operations.
type ExecutorMetrics struct {
	NumStarted   *metric.Counter
	NumSucceeded *metric.Counter
	NumFailed    *metric.Counter
}

// ExecutorPTSMetrics describes metrics related to protected
// timestamp system for executors that maintain PTS records.
type ExecutorPTSMetrics struct {
	NumWithPTS *metric.Gauge
	PTSAge     *metric.Gauge
}

// PTSMetrics is a marker interface indicating that executor metrics
// also keep track of PTS related metrics.
type PTSMetrics interface {
	PTSMetrics() *ExecutorPTSMetrics
}

var _ metric.Struct = &ExecutorMetrics{}
var _ metric.Struct = &ExecutorPTSMetrics{}

// MetricStruct implements metric.Struct interface
func (m *ExecutorMetrics) MetricStruct() {}

// MetricStruct implements metric.Struct interface.
func (m *ExecutorPTSMetrics) MetricStruct() {}

// SchedulerMetrics are metrics specific to job scheduler daemon.
type SchedulerMetrics struct {
	// Number of scheduled jobs started.
	NumStarted *metric.Gauge
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

// MakeExecutorPTSMetrics creates PTS metrics.
func MakeExecutorPTSMetrics(name string) ExecutorPTSMetrics {
	return ExecutorPTSMetrics{
		NumWithPTS: metric.NewGauge(metric.Metadata{
			Name:        fmt.Sprintf("schedules.%s.protected_record_count", name),
			Help:        fmt.Sprintf("Number of PTS records held by %s schedules", name),
			Measurement: "Records",
			Unit:        metric.Unit_COUNT,
			MetricType:  io_prometheus_client.MetricType_GAUGE,
		}),
		PTSAge: metric.NewGauge(metric.Metadata{
			Name:        fmt.Sprintf("schedules.%s.protected_age_sec", name),
			Help:        fmt.Sprintf("The age of the oldest PTS record protected by %s schedules", name),
			Measurement: "Seconds",
			Unit:        metric.Unit_SECONDS,
			MetricType:  io_prometheus_client.MetricType_GAUGE,
		}),
	}
}
