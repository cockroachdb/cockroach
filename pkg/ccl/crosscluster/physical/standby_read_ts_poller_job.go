// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package physical

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfo"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/replication"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

const standbyReadTSPollInterval = 15 * time.Second

type standbyReadTSPollerResumer struct {
	job *jobs.Job

	readerTenant mtinfo.ReadFromTenantInfoAccessor
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
	ctx context.Context, _ interface{}, jobErr error,
) error {
	if jobs.HasErrJobCanceled(jobErr) {
		err := errors.NewAssertionErrorWithWrappedErrf(
			jobErr, "standby read ts poller job is not cancelable",
		)
		log.Errorf(ctx, "%v", err)
	}
	return nil
}

// CollectProfile implements the jobs.Resumer interface.
func (r *standbyReadTSPollerResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

func (r *standbyReadTSPollerResumer) poll(ctx context.Context, execCfg *sql.ExecutorConfig) error {
	ticker := time.NewTicker(standbyReadTSPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			tenantID, replicatedTime, err := r.readerTenant.ReadFromTenantInfo(ctx)
			if err != nil {
				log.Warningf(ctx, "failed to read tenant info: %v", err)
				continue
			}
			if replicatedTime.IsEmpty() {
				continue
			}
			if err = replication.SetupOrAdvanceStandbyReaderCatalog(
				ctx,
				tenantID,
				replicatedTime,
				execCfg.InternalDB,
				execCfg.Settings,
			); err != nil {
				log.Warningf(ctx, "failed to advance replicated timestamp for reader tenant: %v", err)
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
