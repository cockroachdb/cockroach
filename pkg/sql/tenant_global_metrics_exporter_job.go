// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// TenantGlobalMetricsExporterInterval is the interval at which a tenant's
// node in the cluster will update the global metrics. This is exported for
// testing purposes.
var TenantGlobalMetricsExporterInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"tenant_global_metrics_exporter_interval",
	"the interval at which a node in the cluster will update the exported global metrics",
	30*time.Second,
	settings.PositiveDuration,
)

// tenantGlobalMetricsExporter is a singleton job that is meant to be executed
// within SQL nodes for tenants and export global metrics periodically. Given
// that this is a singleton job, this means that only one SQL node will be
// exporting such metrics at any point in time.
//
// Note that global metrics will only be present on a node if the job is running.
// Once the job stops, the metrics will be removed from the registry. While such
// global metrics could be implemented locally by each node, doing so would
// incur unnecessary load on KV (e.g. in the case of aggregating livebytes for
// tenants).
type tenantGlobalMetricsExporter struct {
	job     *jobs.Job
	st      *cluster.Settings
	metrics struct {
		livebytes *metric.Gauge
	}
}

const getTotalLiveBytesQuery = `
SELECT sum(
	(crdb_internal.range_stats(start_key)->>'live_bytes')::INT8 *
	array_length(replicas, 1)
)::INT8 FROM crdb_internal.ranges_no_leases`

var _ jobs.Resumer = (*tenantGlobalMetricsExporter)(nil)

// Resume implements the jobs.Resumer interface.
func (t *tenantGlobalMetricsExporter) Resume(ctx context.Context, execCtx interface{}) error {
	log.Infof(ctx, "starting tenant global metrics exporter job")

	// This job is a forever running background job, and it is always safe to
	// terminate the SQL pod whenever the job is running, so mark it as idle.
	t.job.MarkIdle(true)

	exec := execCtx.(JobExecContext)
	metricsRegistry := exec.ExecCfg().JobRegistry.MetricsRegistry()

	initialRun := true
	defer func() {
		metricsRegistry.RemoveMetric(t.metrics.livebytes)
	}()

	runTask := func() error {
		return exec.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			// Run transaction at low priority to ensure that it does not
			// contend with foreground reads.
			if err := txn.KV().SetUserPriority(roachpb.MinUserPriority); err != nil {
				return err
			}

			// The system tenant is able to view all ranges, including tenant
			// ones. Given that the metrics for the system tenant are tagged
			// with tenant_id="system", exclude secondary tenants from the
			// aggregated metric value.
			query := getTotalLiveBytesQuery
			if exec.ExecCfg().Codec.ForSystemTenant() {
				query += " WHERE start_pretty NOT LIKE '/Tenant/%'"
			}
			row, err := txn.QueryRowEx(
				ctx,
				"get-total-livebytes",
				txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				query,
			)
			if err != nil {
				return errors.Wrap(err, "fetching total livebytes")
			}
			t.metrics.livebytes.Update(int64(tree.MustBeDInt(row[0])))

			// Only register metrics once we get our initial values. This avoids
			// metrics from fluctuating whenever the job restarts.
			if initialRun {
				metricsRegistry.AddMetric(t.metrics.livebytes)
				initialRun = false
			}
			return nil
		})
	}

	timer := timeutil.NewTimer()
	defer timer.Stop()
	for {
		timer.Reset(TenantGlobalMetricsExporterInterval.Get(&exec.ExecCfg().Settings.SV))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			timer.Read = true
			if err := runTask(); err != nil {
				log.Errorf(ctx, "failed error: %v", err)
			}
		}
	}
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (t *tenantGlobalMetricsExporter) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	return nil
}

// CollectProfile implements the jobs.Resumer interface.
func (t *tenantGlobalMetricsExporter) CollectProfile(
	ctx context.Context, execCtx interface{},
) error {
	return nil
}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeAutoTenantGlobalMetricsExporter,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			exporter := &tenantGlobalMetricsExporter{job: job, st: settings}
			exporter.metrics.livebytes = metric.NewGauge(metric.Metadata{
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
