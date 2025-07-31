// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/util/cidr"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// Metrics are for production monitoring of each job type.
type Metrics struct {
	JobMetrics [jobspb.NumJobTypes]*JobTypeMetrics

	// JobSpecificMetrics contains a list of job specific metrics, registered when
	// the job was registered with the system.  Prior to this array, job
	// implementations had to arrange for various hook functions be configured to
	// setup registration of the job specific metrics with the jobs registry. This
	// is a much simpler mechanism. Since having job specific metrics is optional,
	// these metrics are exposed as an array to the metrics registry since metrics
	// arrays may contain nil values. TODO(yevgeniy): Remove hook based
	// implementation of job specific metrics.
	JobSpecificMetrics [jobspb.NumJobTypes]metric.Struct

	// ResolvedMetrics are the per job type metrics for resolved timestamps.
	ResolvedMetrics [jobspb.NumJobTypes]*metric.Gauge

	// RunningNonIdleJobs is the total number of running jobs that are not idle.
	RunningNonIdleJobs *metric.Gauge

	RowLevelTTL  metric.Struct
	Changefeed   metric.Struct
	StreamIngest metric.Struct
	Backup       metric.Struct

	// AdoptIterations counts the number of adopt loops executed by Registry.
	AdoptIterations *metric.Counter

	// ClaimedJobs counts the number of jobs claimed in adopt loops.
	ClaimedJobs *metric.Counter

	// ResumedJobs counts the number of jobs resumed by Registry. It doesn't
	// correlate with the ClaimedJobs counter because a job can be resumed
	// without an adopt loop, e.g., through a StartableJob.
	ResumedJobs *metric.Counter
}

// JobTypeMetrics is a metric.Struct containing metrics for each type of job.
type JobTypeMetrics struct {
	CurrentlyRunning       *metric.Gauge
	CurrentlyIdle          *metric.Gauge
	CurrentlyPaused        *metric.Gauge
	ResumeCompleted        *metric.Counter
	ResumeRetryError       *metric.Counter
	ResumeFailed           *metric.Counter
	FailOrCancelCompleted  *metric.Counter
	FailOrCancelRetryError *metric.Counter
	// TODO (sajjad): FailOrCancelFailed metric is not updated after the modification
	// of retrying all reverting jobs. Remove this metric in v22.1.
	FailOrCancelFailed *metric.Counter

	NumJobsWithPTS *metric.Gauge
	ExpiredPTS     *metric.Counter
	ProtectedAge   *metric.Gauge
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

func makeMetaCurrentlyIdle(typeStr string) metric.Metadata {
	return metric.Metadata{
		Name: fmt.Sprintf("jobs.%s.currently_idle", typeStr),
		Help: fmt.Sprintf("Number of %s jobs currently considered Idle and can be freely shut down",
			typeStr),
		Measurement: "jobs",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
}

func makeMetaCurrentlyPaused(typeStr string) metric.Metadata {
	return metric.Metadata{
		Name: fmt.Sprintf("jobs.%s.currently_paused", typeStr),
		Help: fmt.Sprintf("Number of %s jobs currently considered Paused",
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

func makeMetaProtectedCount(typeStr string) metric.Metadata {
	return metric.Metadata{
		Name:        fmt.Sprintf("jobs.%s.protected_record_count", typeStr),
		Help:        fmt.Sprintf("Number of protected timestamp records held by %s jobs", typeStr),
		Measurement: "records",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
}

func makeMetaProtectedAge(typeStr string) metric.Metadata {
	return metric.Metadata{
		Name:        fmt.Sprintf("jobs.%s.protected_age_sec", typeStr),
		Help:        fmt.Sprintf("The age of the oldest PTS record protected by %s jobs", typeStr),
		Measurement: "seconds",
		Unit:        metric.Unit_SECONDS,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
}

func makeMetaExpiredPTS(typeStr string) metric.Metadata {
	return metric.Metadata{
		Name:        fmt.Sprintf("jobs.%s.expired_pts_records", typeStr),
		Help:        fmt.Sprintf("Number of expired protected timestamp records owned by %s jobs", typeStr),
		Measurement: "records",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
}

var (
	metaAdoptIterations = metric.Metadata{
		Name:        "jobs.adopt_iterations",
		Help:        "number of job-adopt iterations performed by the registry",
		Measurement: "iterations",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}

	metaClaimedJobs = metric.Metadata{
		Name:        "jobs.claimed_jobs",
		Help:        "number of jobs claimed in job-adopt iterations",
		Measurement: "jobs",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}

	metaResumedClaimedJobs = metric.Metadata{
		Name:        "jobs.resumed_claimed_jobs",
		Help:        "number of claimed-jobs resumed in job-adopt iterations",
		Measurement: "jobs",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}

	// MetaRunningNonIdleJobs is the count of currently running jobs that are not
	// reporting as being idle.
	MetaRunningNonIdleJobs = metric.Metadata{
		Name:        "jobs.running_non_idle",
		Help:        "number of running jobs that are not idle",
		Measurement: "jobs",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
)

// MetricStruct implements the metric.Struct interface.
func (Metrics) MetricStruct() {}

// init initializes the metrics for job monitoring.
func (m *Metrics) init(histogramWindowInterval time.Duration, lookup *cidr.Lookup) {
	if MakeRowLevelTTLMetricsHook != nil {
		m.RowLevelTTL = MakeRowLevelTTLMetricsHook(histogramWindowInterval)
	}
	if MakeChangefeedMetricsHook != nil {
		m.Changefeed = MakeChangefeedMetricsHook(histogramWindowInterval, lookup)
	}
	if MakeStreamIngestMetricsHook != nil {
		m.StreamIngest = MakeStreamIngestMetricsHook(histogramWindowInterval)
	}
	if MakeBackupMetricsHook != nil {
		m.Backup = MakeBackupMetricsHook(histogramWindowInterval)
	}

	m.AdoptIterations = metric.NewCounter(metaAdoptIterations)
	m.ClaimedJobs = metric.NewCounter(metaClaimedJobs)
	m.ResumedJobs = metric.NewCounter(metaResumedClaimedJobs)
	m.RunningNonIdleJobs = metric.NewGauge(MetaRunningNonIdleJobs)
	for i := 0; i < jobspb.NumJobTypes; i++ {
		jt := jobspb.Type(i)
		if jt == jobspb.TypeUnspecified { // do not track TypeUnspecified
			continue
		}
		typeStr := strings.ToLower(strings.Replace(jt.String(), " ", "_", -1))
		m.JobMetrics[jt] = &JobTypeMetrics{
			CurrentlyRunning:       metric.NewGauge(makeMetaCurrentlyRunning(typeStr)),
			CurrentlyIdle:          metric.NewGauge(makeMetaCurrentlyIdle(typeStr)),
			CurrentlyPaused:        metric.NewGauge(makeMetaCurrentlyPaused(typeStr)),
			ResumeCompleted:        metric.NewCounter(makeMetaResumeCompeted(typeStr)),
			ResumeRetryError:       metric.NewCounter(makeMetaResumeRetryError(typeStr)),
			ResumeFailed:           metric.NewCounter(makeMetaResumeFailed(typeStr)),
			FailOrCancelCompleted:  metric.NewCounter(makeMetaFailOrCancelCompeted(typeStr)),
			FailOrCancelRetryError: metric.NewCounter(makeMetaFailOrCancelRetryError(typeStr)),
			FailOrCancelFailed:     metric.NewCounter(makeMetaFailOrCancelFailed(typeStr)),
			NumJobsWithPTS:         metric.NewGauge(makeMetaProtectedCount(typeStr)),
			ExpiredPTS:             metric.NewCounter(makeMetaExpiredPTS(typeStr)),
			ProtectedAge:           metric.NewGauge(makeMetaProtectedAge(typeStr)),
		}
		if opts, ok := getRegisterOptions(jt); ok {
			if opts.metrics != nil {
				m.JobSpecificMetrics[jt] = opts.metrics
			}
			if opts.resolvedMetric != nil {
				m.ResolvedMetrics[jt] = opts.resolvedMetric
			}
		}
	}
}

// MakeChangefeedMetricsHook allows for registration of changefeed metrics from
// ccl code.
var MakeChangefeedMetricsHook func(time.Duration, *cidr.Lookup) metric.Struct

// MakeChangefeedMemoryMetricsHook allows for registration of changefeed memory
// metrics from ccl code.
var MakeChangefeedMemoryMetricsHook func(time.Duration) (curCount *metric.Gauge, maxHist metric.IHistogram)

// MakeStreamIngestMetricsHook allows for registration of streaming metrics from
// ccl code.
var MakeStreamIngestMetricsHook func(duration time.Duration) metric.Struct

// MakeRowLevelTTLMetricsHook allows for registration of row-level TTL metrics.
var MakeRowLevelTTLMetricsHook func(time.Duration) metric.Struct

// MakeBackupMetricsHook allows for registration of backup metrics.
var MakeBackupMetricsHook func(time.Duration) metric.Struct

// JobTelemetryMetrics is a telemetry metrics for individual job types.
type JobTelemetryMetrics struct {
	Successful telemetry.Counter
	Failed     telemetry.Counter
	Canceled   telemetry.Counter
}

// newJobTelemetryMetrics creates a new JobTelemetryMetrics object
// for a given job type name.
func newJobTelemetryMetrics(jobName string) *JobTelemetryMetrics {
	return &JobTelemetryMetrics{
		Successful: telemetry.GetCounterOnce(fmt.Sprintf("job.%s.successful", jobName)),
		Failed:     telemetry.GetCounterOnce(fmt.Sprintf("job.%s.failed", jobName)),
		Canceled:   telemetry.GetCounterOnce(fmt.Sprintf("job.%s.canceled", jobName)),
	}
}

// getJobTelemetryMetricsArray initializes an array of job related telemetry
// metrics
func getJobTelemetryMetricsArray() [jobspb.NumJobTypes]*JobTelemetryMetrics {
	var metrics [jobspb.NumJobTypes]*JobTelemetryMetrics
	for i := 0; i < jobspb.NumJobTypes; i++ {
		jt := jobspb.Type(i)
		if jt == jobspb.TypeUnspecified { // do not track TypeUnspecified
			continue
		}
		typeStr := strings.ToLower(strings.Replace(jt.String(), " ", "_", -1))
		metrics[i] = newJobTelemetryMetrics(typeStr)
	}
	return metrics
}

var (
	// TelemetryMetrics contains telemetry metrics for different
	// job types.
	TelemetryMetrics = getJobTelemetryMetricsArray()
	// AbandonedInfoRowsFound is how many times our cleanup
	// routine found abandoned rows.
	AbandonedInfoRowsFound = telemetry.GetCounterOnce("job.registry.cleanup_found_abandoned_rows")
)
