// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package billingjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

var changefeedBillingBytes = metric.NewGauge(metric.Metadata{
	Name:        "changefeed.table_bytes",
	Help:        "Aggregated number of bytes of data per table per changefeed",
	Measurement: "Storage",
	Unit:        metric.Unit_BYTES,
})

type billingJob struct {
	job *jobs.Job
}

// CollectProfile implements jobs.Resumer.
func (j *billingJob) CollectProfile(ctx context.Context, execCtx any) error {
	return nil
}

// OnFailOrCancel implements jobs.Resumer.
func (j *billingJob) OnFailOrCancel(ctx context.Context, execCtx any, jobErr error) error {
	return nil
}

// Resume implements jobs.Resumer.
func (j *billingJob) Resume(ctx context.Context, execCtx any) error {
	exec := execCtx.(sql.JobExecContext)

	// The billing job is a forever running background job. It's always
	// safe to wind the SQL pod down whenever it's running, something we
	// indicate through the job's idle status.
	j.job.MarkIdle(true)

	metricsRegistry := exec.ExecCfg().MetricsRecorder.AppRegistry()
	metricsRegistry.AddMetric(changefeedBillingBytes)

	runMetrics := exec.ExecCfg().JobRegistry.MetricsStruct().JobSpecificMetrics[jobspb.TypeChangefeedBilling].(billingJobMetrics)

	var t timeutil.Timer
	defer t.Stop()

	for {
		t.Reset(changefeedbase.BillingJobInterval.Get(&exec.ExecCfg().Settings.SV))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-t.C:
			t.Read = true
			// TODO: add timeout?
			if err := j.updateChangefeedBillingMetrics(ctx, execCtx.(sql.JobExecContext)); err != nil {
				log.Errorf(ctx, "error updating changefeed billing metrics: %v", err)
				runMetrics.NumErrors.Inc(1)
			} else {
				runMetrics.LastRun.Update(now.UnixNano())
				runMetrics.Latency.RecordValue(timeutil.Since(now).Nanoseconds())
			}
		}
	}
}

func (j *billingJob) updateChangefeedBillingMetrics(ctx context.Context, execCtx sql.JobExecContext) error {
	bytes, err := FetchChangefeedBillingBytes(ctx, execCtx)
	if err != nil {
		return err
	}

	changefeedBillingBytes.Update(bytes)
	return nil
}

var _ jobs.Resumer = &billingJob{}

type billingJobMetrics struct {
	NumErrors *metric.Counter
	Latency   metric.IHistogram
	LastRun   *metric.Gauge
}

func (m billingJobMetrics) MetricStruct() {}

func newBillingJobMetrics() billingJobMetrics {
	return billingJobMetrics{
		NumErrors: metric.NewCounter(metric.Metadata{
			Name:        "cdc.billing_job.errors",
			Help:        "Number of errors encountered by the billing job",
			Measurement: "errors",
			Unit:        metric.Unit_COUNT,
			MetricType:  io_prometheus_client.MetricType_COUNTER,
		}),
		Latency: metric.NewHistogram(metric.HistogramOptions{
			Metadata: metric.Metadata{
				Name:        "cdc.billing_job.latency", // TODO: is there already a job latency by type metric?
				Help:        "Latency of the billing job",
				Measurement: "Latency",
				Unit:        metric.Unit_NANOSECONDS,
				MetricType:  io_prometheus_client.MetricType_HISTOGRAM,
			},
			Mode:         metric.HistogramModePrometheus,
			Duration:     base.DefaultHistogramWindowInterval(),
			BucketConfig: metric.BatchProcessLatencyBuckets,
		}),
		LastRun: metric.NewGauge(metric.Metadata{
			Name:        "cdc.billing_job.last_run",
			Help:        "Timestamp of the last run of the billing job (in nanoseconds)",
			Measurement: "time",
			Unit:        metric.Unit_TIMESTAMP_NS,
			MetricType:  io_prometheus_client.MetricType_GAUGE,
		}),
	}
}

// FetchChangefeedBillingBytes fetches the total number of bytes of data watched
// by all changefeeds. It counts tables that are watched by multiple changefeeds
// multiple times.
func FetchChangefeedBillingBytes(ctx context.Context, execCtx sql.JobExecContext) (int64, error) {
	deets, err := getChangefeedDetails(ctx, execCtx)
	if err != nil {
		return 0, err
	}

	feedsTableIds := make(map[int][]descpb.ID, len(deets))
	tableIDs := make([]descpb.ID, 0, len(deets))
	for cdi, cd := range deets {
		// check both possible locations for table data due to older proto version. TODO: is this still necessary?
		// inspired by AllTargets in changefeedccl/changefeed.go
		if len(cd.TargetSpecifications) > 0 {
			for _, ts := range cd.TargetSpecifications {
				if ts.TableID > 0 {
					feedsTableIds[cdi] = append(feedsTableIds[cdi], ts.TableID)
					tableIDs = append(tableIDs, ts.TableID)
				}
			}
		} else {
			for id := range cd.Tables {
				feedsTableIds[cdi] = append(feedsTableIds[cdi], id)
				tableIDs = append(tableIDs, id)
			}
		}
	}

	tableSizes, err := fetchTableSizes(ctx, execCtx, tableIDs)
	if err != nil {
		return 0, err
	}

	var total int64
	for _, tableIds := range feedsTableIds {
		for _, id := range tableIds {
			total += tableSizes[id]
		}
	}
	return total, nil
}

func fetchTableSizes(ctx context.Context, execCtx sql.JobExecContext, tableIDs []descpb.ID) (map[descpb.ID]int64, error) {
	tableSizes := make(map[descpb.ID]int64, len(tableIDs))

	type spanInfo struct {
		span  roachpb.Span
		table descpb.ID
	}

	// build list of spans for all tables
	spanSizes := make(map[string]spanInfo, len(tableIDs))
	spans := make([]roachpb.Span, 0, len(tableIDs))
	for _, id := range tableIDs {
		// fetch table descriptor
		var desc catalog.TableDescriptor
		fetchTableDesc := func(
			ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
		) error {
			tableDesc, err := descriptors.ByID(txn.KV()).WithoutNonPublic().Get().Table(ctx, id)
			if err != nil {
				return err
			}
			desc = tableDesc
			return nil
		}
		if err := sql.DescsTxn(ctx, execCtx.ExecCfg(), fetchTableDesc); err != nil {
			if errors.Is(err, catalog.ErrDescriptorDropped) {
				// if the table was dropped, we can ignore it this cycle
				continue
			}
			return nil, err
		}

		// TODO: do we need to count the sizes of other indexes?
		span := desc.PrimaryIndexSpan(execCtx.ExecCfg().Codec)
		spans = append(spans, span)
		spanSizes[span.String()] = spanInfo{span: span, table: id}
	}

	// fetch span stats and fill in table sizes
	resp, err := execCtx.ExecCfg().TenantStatusServer.SpanStats(
		ctx,
		&roachpb.SpanStatsRequest{
			NodeID: "0", // fan out
			Spans:  spans,
		},
	)
	if err != nil {
		return nil, err
	}
	for spanStr, stats := range resp.SpanToStats {
		si := spanSizes[spanStr]
		tableSizes[si.table] += stats.ApproximateTotalStats.LiveBytes
	}

	return tableSizes, nil
}

const changefeedDetailsQuery = `
	SELECT j.id, ji.value
	FROM system.jobs j JOIN system.job_info ji ON j.id = ji.job_id
	WHERE status IN ('running', 'paused') AND job_type = 'CHANGEFEED'
		AND info_key = '` + jobs.LegacyPayloadKey + `'
`

// getChangefeedDetails fetches the changefeed details for all changefeeds.
func getChangefeedDetails(ctx context.Context, execCtx sql.JobExecContext) ([]*jobspb.ChangefeedDetails, error) {
	var deets []*jobspb.ChangefeedDetails
	err := execCtx.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		it, err := txn.QueryIteratorEx(ctx, "changefeeds_billing_payloads", txn.KV(), sessiondata.NodeUserSessionDataOverride, changefeedDetailsQuery)
		if err != nil {
			return err
		}
		defer func() { _ = it.Close() }()

		var ok bool
		for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
			row := it.Cur()
			id, payloadBs := int64(tree.MustBeDInt(row[0])), tree.MustBeDBytes(row[1])
			var payload jobspb.Payload
			if err := payload.Unmarshal([]byte(payloadBs)); err != nil {
				return errors.WithDetailf(err, "failed to unmarshal payload for job %d", id)
			}

			details := payload.GetDetails()
			if details == nil {
				return errors.AssertionFailedf("no details for job %d", id)
			}
			cfDetails, ok := details.(*jobspb.Payload_Changefeed)
			if !ok {
				return errors.AssertionFailedf("unexpected details type %T for job %d", details, id)
			}
			deets = append(deets, cfDetails.Changefeed)
		}
		return err
	})

	return deets, err
}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &billingJob{job: job}
	}

	jobs.RegisterConstructor(jobspb.TypeChangefeedBilling, createResumerFn,
		jobs.DisablesTenantCostControl, jobs.WithJobMetrics(newBillingJobMetrics()))
}
