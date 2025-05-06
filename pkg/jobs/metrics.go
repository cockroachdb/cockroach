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

func typeToString(jobType jobspb.Type) string {
	return strings.ToLower(strings.Replace(jobType.String(), " ", "_", -1))
}

func makeMetaCurrentlyRunning(jt jobspb.Type) metric.Metadata {
	typeStr := typeToString(jt)
	m := metric.Metadata{
		Name: fmt.Sprintf("jobs.%s.currently_running", typeStr),
		Help: fmt.Sprintf("Number of %s jobs currently running in Resume or OnFailOrCancel state",
			typeStr),
		Measurement: "jobs",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}

	switch jt {
	case jobspb.TypeAutoCreateStats:
		m.Essential = true
		m.Category = metric.Metadata_SQL
		m.HowToUse = `This metric tracks the number of active automatically generated statistics jobs that could also be consuming resources. Ensure that foreground SQL traffic is not impacted by correlating this metric with SQL latency and query volume metrics.`
	case jobspb.TypeCreateStats:
		m.Essential = true
		m.Category = metric.Metadata_SQL
		m.HowToUse = `This metric tracks the number of active create statistics jobs that may be consuming resources. Ensure that foreground SQL traffic is not impacted by correlating this metric with SQL latency and query volume metrics.`
	case jobspb.TypeBackup:
		m.Essential = true
		m.Category = metric.Metadata_SQL
		m.HowToUse = `See Description.`
	case jobspb.TypeRowLevelTTL:
		m.Essential = true
		m.Category = metric.Metadata_TTL
		m.HowToUse = `Monitor this metric to ensure there are not too many Row Level TTL jobs running at the same time. Generally, this metric should be in the low single digits.`
	}

	return m
}

func makeMetaCurrentlyIdle(jt jobspb.Type) metric.Metadata {
	typeStr := typeToString(jt)
	return metric.Metadata{
		Name: fmt.Sprintf("jobs.%s.currently_idle", typeStr),
		Help: fmt.Sprintf("Number of %s jobs currently considered Idle and can be freely shut down",
			typeStr),
		Measurement: "jobs",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
}

func makeMetaCurrentlyPaused(jt jobspb.Type) metric.Metadata {
	typeStr := typeToString(jt)
	m := metric.Metadata{
		Name: fmt.Sprintf("jobs.%s.currently_paused", typeStr),
		Help: fmt.Sprintf("Number of %s jobs currently considered Paused",
			typeStr),
		Measurement: "jobs",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
	switch jt {
	case jobspb.TypeAutoCreateStats:
		m.Essential = true
		m.Category = metric.Metadata_SQL
		m.HowToUse = `This metric is a high-level indicator that automatically generated statistics jobs are paused which can lead to the query optimizer running with stale statistics. Stale statistics can cause suboptimal query plans to be selected leading to poor query performance.`
	case jobspb.TypeBackup:
		m.Essential = true
		m.Category = metric.Metadata_SQL
		m.HowToUse = `Monitor and alert on this metric to safeguard against an inadvertent operational error of leaving a backup job in a paused state for an extended period of time. In functional areas, a paused job can hold resources or have concurrency impact or some other negative consequence. Paused backup may break the recovery point objective (RPO).`
	case jobspb.TypeChangefeed:
		m.Essential = true
		m.Category = metric.Metadata_CHANGEFEEDS
		m.HowToUse = `Monitor and alert on this metric to safeguard against an inadvertent operational error of leaving a changefeed job in a paused state for an extended period of time. Changefeed jobs should not be paused for a long time because the protected timestamp prevents garbage collection.`
	case jobspb.TypeRowLevelTTL:
		m.Essential = true
		m.Category = metric.Metadata_TTL
		m.HowToUse = `Monitor this metric to ensure the Row Level TTL job does not remain paused inadvertently for an extended period.`
	}
	return m
}

func makeMetaResumeCompeted(jt jobspb.Type) metric.Metadata {
	typeStr := typeToString(jt)
	m := metric.Metadata{
		Name: fmt.Sprintf("jobs.%s.resume_completed", typeStr),
		Help: fmt.Sprintf("Number of %s jobs which successfully resumed to completion",
			typeStr),
		Measurement: "jobs",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}

	switch jt {
	case jobspb.TypeRowLevelTTL:
		m.Essential = true
		m.Category = metric.Metadata_TTL
		m.HowToUse = `If Row Level TTL is enabled, this metric should be nonzero and correspond to the ttl_cron setting that was chosen. If this metric is zero, it means the job is not running`
	}
	return m
}

func makeMetaResumeRetryError(jt jobspb.Type) metric.Metadata {
	typeStr := typeToString(jt)
	return metric.Metadata{
		Name: fmt.Sprintf("jobs.%s.resume_retry_error", typeStr),
		Help: fmt.Sprintf("Number of %s jobs which failed with a retriable error",
			typeStr),
		Measurement: "jobs",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
}

func makeMetaResumeFailed(jt jobspb.Type) metric.Metadata {
	typeStr := typeToString(jt)
	m := metric.Metadata{
		Name: fmt.Sprintf("jobs.%s.resume_failed", typeStr),
		Help: fmt.Sprintf("Number of %s jobs which failed with a non-retriable error",
			typeStr),
		Measurement: "jobs",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}

	switch jt {
	case jobspb.TypeAutoCreateStats:
		m.Essential = true
		m.Category = metric.Metadata_SQL
		m.HowToUse = `This metric is a high-level indicator that automatically generated table statistics is failing. Failed statistic creation can lead to the query optimizer running with stale statistics. Stale statistics can cause suboptimal query plans to be selected leading to poor query performance.`
	case jobspb.TypeRowLevelTTL:
		m.Essential = true
		m.Category = metric.Metadata_TTL
		m.HowToUse = `This metric should remain at zero. Repeated errors means the Row Level TTL job is not deleting data.`
	}
	return m
}

func makeMetaFailOrCancelCompeted(jt jobspb.Type) metric.Metadata {
	typeStr := typeToString(jt)
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

func makeMetaFailOrCancelRetryError(jt jobspb.Type) metric.Metadata {
	typeStr := typeToString(jt)
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

func makeMetaFailOrCancelFailed(jt jobspb.Type) metric.Metadata {
	typeStr := typeToString(jt)
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

func makeMetaProtectedCount(jt jobspb.Type) metric.Metadata {
	typeStr := typeToString(jt)
	return metric.Metadata{
		Name:        fmt.Sprintf("jobs.%s.protected_record_count", typeStr),
		Help:        fmt.Sprintf("Number of protected timestamp records held by %s jobs", typeStr),
		Measurement: "records",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
}

func makeMetaProtectedAge(jt jobspb.Type) metric.Metadata {
	typeStr := typeToString(jt)
	m := metric.Metadata{
		Name:        fmt.Sprintf("jobs.%s.protected_age_sec", typeStr),
		Help:        fmt.Sprintf("The age of the oldest PTS record protected by %s jobs", typeStr),
		Measurement: "seconds",
		Unit:        metric.Unit_SECONDS,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}

	switch jt {
	case jobspb.TypeChangefeed:
		m.Essential = true
		m.Category = metric.Metadata_CHANGEFEEDS
		m.HowToUse = `Changefeeds use protected timestamps to protect the data from being garbage collected. Ensure the protected timestamp age does not significantly exceed the GC TTL zone configuration. Alert on this metric if the protected timestamp age is greater than 3 times the GC TTL.`
	}

	return m
}

func makeMetaExpiredPTS(jt jobspb.Type) metric.Metadata {
	typeStr := typeToString(jt)
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
		m.JobMetrics[jt] = &JobTypeMetrics{
			CurrentlyRunning:       metric.NewGauge(makeMetaCurrentlyRunning(jt)),
			CurrentlyIdle:          metric.NewGauge(makeMetaCurrentlyIdle(jt)),
			CurrentlyPaused:        metric.NewGauge(makeMetaCurrentlyPaused(jt)),
			ResumeCompleted:        metric.NewCounter(makeMetaResumeCompeted(jt)),
			ResumeRetryError:       metric.NewCounter(makeMetaResumeRetryError(jt)),
			ResumeFailed:           metric.NewCounter(makeMetaResumeFailed(jt)),
			FailOrCancelCompleted:  metric.NewCounter(makeMetaFailOrCancelCompeted(jt)),
			FailOrCancelRetryError: metric.NewCounter(makeMetaFailOrCancelRetryError(jt)),
			FailOrCancelFailed:     metric.NewCounter(makeMetaFailOrCancelFailed(jt)),
			NumJobsWithPTS:         metric.NewGauge(makeMetaProtectedCount(jt)),
			ExpiredPTS:             metric.NewCounter(makeMetaExpiredPTS(jt)),
			ProtectedAge:           metric.NewGauge(makeMetaProtectedAge(jt)),
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
