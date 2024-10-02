// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/replication"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var standbyReadTSPollInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"bulkio.stream_ingestion.standby_read_ts_poll_interval",
	"the interval at which a StandbyReadTSPoller job polls for replicated timestamp",
	15*time.Second,
	settings.PositiveDuration)

type standbyReadTSPollerResumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*standbyReadTSPollerResumer)(nil)

// Resume implements the jobs.Resumer interface.
func (r *standbyReadTSPollerResumer) Resume(
	ctx context.Context, execCtxI interface{},
) (jobErr error) {
	execCtx := execCtxI.(sql.JobExecContext)
	return r.poll(ctx, execCtx.ExecCfg())
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *standbyReadTSPollerResumer) OnFailOrCancel(
	_ context.Context, _ interface{}, jobErr error,
) error {
	if jobs.HasErrJobCanceled(jobErr) {
		return errors.NewAssertionErrorWithWrappedErrf(
			jobErr,
			"standby read ts poller job is not cancelable")
	}
	return nil
}

// CollectProfile implements the jobs.Resumer interface.
func (r *standbyReadTSPollerResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

func (r *standbyReadTSPollerResumer) poll(ctx context.Context, execCfg *sql.ExecutorConfig) error {
	ticker := time.NewTicker(standbyReadTSPollInterval.Get(&execCfg.Settings.SV))
	defer ticker.Stop()
	var previousReplicatedTimestamp hlc.Timestamp

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			tenantInfoAccessor := execCfg.UpgradeJobDeps.SystemDeps().TenantInfoAccessor
			if tenantInfoAccessor == nil {
				return errors.AssertionFailedf("tenant info accessor cannot be nil")
			}
			tenantID, replicatedTime, err := tenantInfoAccessor.ReadFromTenantInfo(ctx)
			if err != nil {
				log.Warningf(ctx, "failed to read tenant info of tenant {%d}: %v", tenantID, err)
				continue
			}
			// No need to call SetupOrAdvanceStandbyReaderCatalog() if
			// replicated time has not advanced.
			if replicatedTime.Equal(previousReplicatedTimestamp) {
				continue
			}
			previousReplicatedTimestamp = replicatedTime
			if err = replication.SetupOrAdvanceStandbyReaderCatalog(
				ctx,
				tenantID,
				replicatedTime,
				execCfg.InternalDB,
				execCfg.Settings,
			); err != nil {
				log.Warningf(ctx, "failed to advance replicated timestamp for reader tenant {%d}: %v", tenantID, err)
			}
		}
	}
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeStandbyReadTSPoller,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &standbyReadTSPollerResumer{
				job: job,
			}
		},
		jobs.UsesTenantCostControl,
	)
}
