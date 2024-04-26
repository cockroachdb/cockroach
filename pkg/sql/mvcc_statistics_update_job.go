// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package upgrades contains the implementation of upgrades. It is imported
// by the server library.
//
// This package registers the upgrades with the upgrade package.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// TenantGlobalMetricsExporterInterval is the interval at which an external
// tenant's process in the cluster will update the global metrics, and is
// measured from the *last update*. This is exported for testing purposes.
var TenantGlobalMetricsExporterInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"tenant_global_metrics_exporter_interval",
	"the interval at which a node in the cluster will update the exported global metrics",
	60*time.Second,
	settings.PositiveDuration,
)

// mvccStatisticsUpdateJob is a singleton job that is meant to update MVCC
// statistics. Historically, this was added to update system.mvcc_statistics,
// but the project was deprioritized. Currently, this is used by external
// process tenants to export global metrics periodically. For such metrics,
// they will only be present on a SQL node if the job is running. Once the job
// stops, the metrics will be removed from the metric registry.
type mvccStatisticsUpdateJob struct {
	job *jobs.Job
	st  *cluster.Settings

	// dynamicMetrics keep track of metrics which are added/removed dynamically
	// as the job runs. Unlike regular job metrics (i.e. WithJobMetrics), which
	// are registered when the job starts the first time, and never removed from
	// the metric registry, metrics in this list should be removed when the job
	// is not running.
	dynamicMetrics struct {
		livebytes *metric.Gauge
		// tableFeedBytes is a the sum of each table in each (enterprise)
		// changefeed's live bytes. Tables in multiple changefeeds will be
		// counted multiple times. This metric will be used for billing.
		tableChangefeedBytes *metric.Gauge
	}
}

var _ jobs.Resumer = (*mvccStatisticsUpdateJob)(nil)

// Resume implements the jobs.Resumer interface.
func (j *mvccStatisticsUpdateJob) Resume(ctx context.Context, execCtxI interface{}) error {
	log.Infof(ctx, "starting mvcc statistics update job")

	// This job is a forever running background job, and it is always safe to
	// terminate the SQL pod whenever the job is running, so mark it as idle.
	j.job.MarkIdle(true)

	execCtx := execCtxI.(JobExecContext)

	// Export global metrics for tenants if this is an out-of-process SQL node.
	// All external mode tenant servers have no node IDs.
	if _, hasNodeID := execCtx.ExecCfg().NodeInfo.NodeID.OptionalNodeID(); !hasNodeID {
		return j.runTenantGlobalMetricsExporter(ctx, execCtx)
	}

	// TODO(zachlite):
	// Delete samples older than configurable setting...
	// Collect span stats for tenant descriptors...
	// Write new samples...

	// Block until context is cancelled since there's nothing that needs to be
	// done here. We should not return nil, or else the job will be marked as
	// succeeded.
	<-ctx.Done()
	return ctx.Err()
}

// runTenantGlobalMetricsExporter executes the logic to export global metrics
// for tenants.
func (j *mvccStatisticsUpdateJob) runTenantGlobalMetricsExporter(
	ctx context.Context, execCtx JobExecContext,
) error {
	metricsRegistry := execCtx.ExecCfg().MetricsRecorder.AppRegistry()

	initialRun := true
	defer func() {
		metricsRegistry.RemoveMetric(j.dynamicMetrics.livebytes)
		metricsRegistry.RemoveMetric(j.dynamicMetrics.tableChangefeedBytes)
	}()

	// TODO: this queries the whole keyspace so we cant actually reuse this result
	runTask := func() error {
		resp, err := execCtx.ExecCfg().TenantStatusServer.SpanStats(
			ctx,
			&roachpb.SpanStatsRequest{
				// Fan out to all nodes. SpanStats takes care of only contacting
				// the relevant nodes with the tenant's span.
				NodeID: "0",
				Spans:  []roachpb.Span{execCtx.ExecCfg().Codec.TenantSpan()},
			},
		)
		if err != nil {
			return err
		}
		var total int64
		for _, stats := range resp.SpanToStats {
			total += stats.ApproximateTotalStats.LiveBytes
		}
		j.dynamicMetrics.livebytes.Update(total)

		// get enterprise changefeeds with data
		var deets []jobspb.ChangefeedDetails // TODO: get this somehow

		// get table ids per changefeed (by idx in above). supports old and new pb versions
		// also set up table info map
		feedsTableIds := make(map[int][]descpb.ID, len(deets))
		tableSizes := make(map[descpb.ID]int64, len(deets))
		for cdi, cd := range deets {
			if len(cd.TargetSpecifications) > 0 {
				for _, ts := range cd.TargetSpecifications {
					if ts.TableID > 0 {
						feedsTableIds[cdi] = append(feedsTableIds[cdi], ts.TableID)
						tableSizes[ts.TableID] = 0
					}
				}
			} else {
				for id := range cd.Tables {
					feedsTableIds[cdi] = append(feedsTableIds[cdi], id)
					tableSizes[id] = 0
				}
			}
		}

		// fetch & fill in table descriptors
		for id := range tableSizes {
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
			if err := DescsTxn(ctx, execCtx.ExecCfg(), fetchTableDesc); err != nil {
				if errors.Is(err, catalog.ErrDescriptorDropped) {
					// TODO: ignore this and continue?
					return nil
				}
				return err
			}

			// TODO: do we need to count the sizes of other indexes?
			span := desc.PrimaryIndexSpan(execCtx.ExecCfg().Codec)

			// TODO: do this once for all spans
			resp, err := execCtx.ExecCfg().TenantStatusServer.SpanStats(
				ctx,
				&roachpb.SpanStatsRequest{
					NodeID: "0", // fan out
					Spans:  []roachpb.Span{span},
				},
			)
			if err != nil {
				return err
			}

			for _, stats := range resp.SpanToStats {
				tableSizes[id] += stats.ApproximateTotalStats.LiveBytes
			}
		}

		var total2 int64
		for _, tableIds := range feedsTableIds {
			for _, id := range tableIds {
				total2 += tableSizes[id]
			}
		}
		j.dynamicMetrics.tableChangefeedBytes.Update(total2)

		// Only register metrics once we get our initial values. This avoids
		// metrics from fluctuating whenever the job restarts.
		if initialRun {
			metricsRegistry.AddMetric(j.dynamicMetrics.livebytes)
			metricsRegistry.AddMetric(j.dynamicMetrics.tableChangefeedBytes)
			initialRun = false
		}
		return nil
	}

	var timer timeutil.Timer
	defer timer.Stop()

	// Fire the timer immediately to start the initial update.
	timer.Reset(0)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			timer.Read = true
			if err := runTask(); err != nil {
				log.Errorf(ctx, "mvcc statistics update job error: %v", err)
			}
			timer.Reset(TenantGlobalMetricsExporterInterval.Get(&execCtx.ExecCfg().Settings.SV))
		}
	}
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (j *mvccStatisticsUpdateJob) OnFailOrCancel(
	ctx context.Context, _ interface{}, jobErr error,
) error {
	if jobs.HasErrJobCanceled(jobErr) {
		err := errors.NewAssertionErrorWithWrappedErrf(
			jobErr, "mvcc statistics update job is not cancelable",
		)
		log.Errorf(ctx, "%v", err)
	}
	return nil
}

// CollectProfile implements the jobs.Resumer interface.
func (j *mvccStatisticsUpdateJob) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

// func tableSpans(execCtx JobExecContext, tableDescs []catalog.TableDescriptor) roachpb.Spans {
// 	var trackedSpans []roachpb.Span
// 	for _, d := range tableDescs {
// 		trackedSpans = append(trackedSpans, d.PrimaryIndexSpan(execCtx.ExecCfg().Codec))
// 	}
// 	return trackedSpans
// }

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeMVCCStatisticsUpdate,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			exporter := &mvccStatisticsUpdateJob{job: job, st: settings}
			exporter.dynamicMetrics.livebytes = metric.NewGauge(metric.Metadata{
				Name:        "sql.aggregated_livebytes",
				Help:        "Aggregated number of bytes of live data (keys plus values)",
				Measurement: "Storage",
				Unit:        metric.Unit_BYTES,
			})
			return exporter
		},
		jobs.DisablesTenantCostControl,
	)
}
