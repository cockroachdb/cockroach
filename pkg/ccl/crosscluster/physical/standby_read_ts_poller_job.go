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

/*

The reader tenant does not current support running backups or changefeeds, even
though these are ostensibly read-only operations.

# BACKUPS

To support backups from a reader, we would need to do one of the following:
  1) Extend BACKUP to read the catalog from the standby tenant in every place it
     reads the catalog (there are many) and be aware of the timestamps at which
     it can do so and backup before it plans.
  2) Extend backup to note external row data indirection in the descriptors it
     reads and backs up from the catalog of the reader, obey it when reading
     rows but make the rows appear _as if_ they were read from the reader table
     desc's span instead.
  3) Extend backup to be aware of external row data indirection in the descs it
     reads from the reader tenant catalog, and extend _restore_ to look for data
     in external spans when restoring.

Alternatively, we could simply not allow backing up from the reader tenant, and
instead allow backing up the whole standby tenant from system tenant so long as
that backup's end time is <= the replicated time. This would be simple, but is
only really useful for many customers if paired with extensions to RESTORE to
allow restoring tables or databases from such a backup of a whole tenant.

# CDC

To support CDC from the standby cluster, the fact that rangefeeds can emit
timestamps that a non-mvcc AddSSTable can write under is a problem that makes
using a rangefeed on the ingested standby difficult.

1) We could solve this by taking manual control of the closed timestamp
   infrastructure, and advancing it only when the the replicated time advances.
   This likely requires substantial changes in KV.

2) We could also ignore the ingested data completely and just emit to the CDC
   sink as an additional side-effect of processing the changed KVs in the
   replication process, in addition to writing them, i.e. feed each KV in the
   CDC filter/encode/emit pipeline after writing it. This however tightly
   couples the CDC emission to replication -- if we cannot emit to the sink, we
   cannot advance the replicated time either. Additionally we could not support
   some behaviors offered by CDC like re-emitting every row after a schema
   change.

3) We could instead alter CDC to be able to use a "Cross-cluster rangefeed"
	 backed by the existing PCR/LDR event stream, that it opens on demand on the
	 key spans in the primary that it wants to watch, at the timestamp cursor it
	 needs. Opening a second, independent stream rather than trying to use the
	 events replicated by the existing PCR stream would allow CDC to pick the time
	 it wants when it wants, for example to allow for sink unavailability or watch
	 a new span and backfill after a schema change. This approach potentially
	 doubles the data transfer between the clusters and doubles the processing of
	 some of the parts of the stream in the consuming cluster to the point where
	 CDC and PCR diverge, but imposes minimal changes on CDC's behavior and PCR's
	 reliability.
*/

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
			ticker.Reset(standbyReadTSPollInterval.Get(&execCfg.Settings.SV))
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
