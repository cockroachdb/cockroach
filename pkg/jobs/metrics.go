// Copyright 2018 The Cockroach Authors.
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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// Metrics are for production monitoring of each job type.
type Metrics struct {
	JobMetrics [jobspb.NumJobTypes]*JobTypeMetrics

	Changefeed metric.Struct
}

// JobTypeMetrics is a metric.Struct containing metrics for each type of job.
type JobTypeMetrics struct {
	CurrentlyRunning       *metric.Gauge
	ResumeCompleted        *metric.Counter
	ResumeRetryError       *metric.Counter
	ResumeFailed           *metric.Counter
	FailOrCancelCompleted  *metric.Counter
	FailOrCancelRetryError *metric.Counter
	FailOrCancelFailed     *metric.Counter
}

// MetricStruct implements the metric.Struct interface.
func (JobTypeMetrics) MetricStruct() {}

func makeMetaCurrentlyRunning(typeStr string) metric.Metadata {
	return metric.Metadata{
		Name: fmt.Sprintf("jobs.%s.currently_running", typeStr),
		Help: fmt.Sprintf("Number of %s jobs currently running in Resume or OnFailOrCancel state",
			typeStr),
		Measurement: "jobs",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
}

func makeMetaResumeCompeted(typeStr string) metric.Metadata {
	return metric.Metadata{
		Name: fmt.Sprintf("jobs.%s.resume_completed", typeStr),
		Help: fmt.Sprintf("Number of %s jobs which successfully resumed to completion",
			typeStr),
		Measurement: "jobs",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
}

func makeMetaResumeRetryError(typeStr string) metric.Metadata {
	return metric.Metadata{
		Name: fmt.Sprintf("jobs.%s.resume_retry_error", typeStr),
		Help: fmt.Sprintf("Number of %s jobs which failed with a retriable error",
			typeStr),
		Measurement: "jobs",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
}

func makeMetaResumeFailed(typeStr string) metric.Metadata {
	return metric.Metadata{
		Name: fmt.Sprintf("jobs.%s.resume_failed", typeStr),
		Help: fmt.Sprintf("Number of %s jobs which failed with a non-retriable error",
			typeStr),
		Measurement: "jobs",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
}

func makeMetaFailOrCancelCompeted(typeStr string) metric.Metadata {
	return metric.Metadata{
		Name: fmt.Sprintf("jobs.%s.fail_or_cancel_completed", typeStr),
		Help: fmt.Sprintf("Number of %s jobs which successfully completed "+
			"their failure or cancelation process",
			typeStr),
		Measurement: "jobs",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
}

func makeMetaFailOrCancelRetryError(typeStr string) metric.Metadata {
	return metric.Metadata{
		Name: fmt.Sprintf("jobs.%s.fail_or_cancel_retry_error", typeStr),
		Help: fmt.Sprintf("Number of %s jobs which failed with a retriable "+
			"error on their failure or cancelation process",
			typeStr),
		Measurement: "jobs",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
}

func makeMetaFailOrCancelFailed(typeStr string) metric.Metadata {
	return metric.Metadata{
		Name: fmt.Sprintf("jobs.%s.fail_or_cancel_failed", typeStr),
		Help: fmt.Sprintf("Number of %s jobs which failed with a "+
			"non-retriable error on their failure or cancelation process",
			typeStr),
		Measurement: "jobs",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
}

// MetricStruct implements the metric.Struct interface.
func (Metrics) MetricStruct() {}

// init initializes the metrics for job monitoring.
func (m *Metrics) init(histogramWindowInterval time.Duration) {
	if MakeChangefeedMetricsHook != nil {
		m.Changefeed = MakeChangefeedMetricsHook(histogramWindowInterval)
	}
	for i := 0; i < jobspb.NumJobTypes; i++ {
		jt := jobspb.Type(i)
		if jt == jobspb.TypeUnspecified { // do not track TypeUnspecified
			continue
		}
		typeStr := strings.ToLower(strings.Replace(jt.String(), " ", "_", -1))
		m.JobMetrics[jt] = &JobTypeMetrics{
			CurrentlyRunning:       metric.NewGauge(makeMetaCurrentlyRunning(typeStr)),
			ResumeCompleted:        metric.NewCounter(makeMetaResumeCompeted(typeStr)),
			ResumeRetryError:       metric.NewCounter(makeMetaResumeRetryError(typeStr)),
			ResumeFailed:           metric.NewCounter(makeMetaResumeFailed(typeStr)),
			FailOrCancelCompleted:  metric.NewCounter(makeMetaFailOrCancelCompeted(typeStr)),
			FailOrCancelRetryError: metric.NewCounter(makeMetaFailOrCancelRetryError(typeStr)),
			FailOrCancelFailed:     metric.NewCounter(makeMetaFailOrCancelFailed(typeStr)),
		}
	}
}

// MakeChangefeedMetricsHook allows for registration of changefeed metrics from
// ccl code.
var MakeChangefeedMetricsHook func(time.Duration) metric.Struct
