// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rgreconciler

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup/rgkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/errors"
)

type resumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*resumer)(nil)

// Resume implements jobs.Resumer. The job is NonCancelable; returning
// any error (without marking it permanent) puts the job back in the
// retry queue, so it never enters a terminal failed state. Local
// errors from the rangefeed are already logged and retried inside
// rangefeedcache; only setup errors flow up here.
func (r *resumer) Resume(ctx context.Context, execCtxI interface{}) error {
	execCtx := execCtxI.(sql.JobExecContext)
	cfg := execCtx.ExecCfg()
	r.job.MarkIdle(true)

	if err := resourcegroup.WaitForTenantResourceGroupsTable(ctx, cfg.Settings, cfg.DistSQLSrv.Stopper); err != nil {
		return err
	}

	var pusher resourcegroup.Pusher
	if cfg.Codec.ForSystemTenant() {
		w := rgkvaccessor.NewWriter(cfg.InternalDB)
		pusher = newLocalPusher(w, roachpb.SystemTenantID)
	} else {
		if cfg.TenantConnector == nil {
			return errors.AssertionFailedf("rgreconciler: app tenant has no kvtenant.Connector on ExecCfg")
		}
		pusher = newConnectorPusher(cfg.TenantConnector)
	}

	rec := New(
		cfg.Codec,
		cfg.Clock,
		cfg.RangeFeedFactory,
		cfg.DistSQLSrv.Stopper,
		cfg.SystemTableIDResolver,
		pusher,
	)
	return rec.Reconcile(ctx)
}

// OnFailOrCancel implements jobs.Resumer. Nothing to clean up.
func (r *resumer) OnFailOrCancel(context.Context, interface{}, error) error { return nil }

// CollectProfile implements jobs.Resumer. Nothing to collect.
func (r *resumer) CollectProfile(context.Context, interface{}) error { return nil }

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeResourceGroupReconciliation,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return &resumer{job: job}
		},
		jobs.DisablesTenantCostControl,
	)
}
