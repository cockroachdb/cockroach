package zcfgreconciler

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/errors"
)

func init() {
	jobs.RegisterConstructor(jobspb.TypeAutoZoneConfigReconciliation,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &resumer{job: job}
		})
}

type resumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*resumer)(nil)

// Resume implements the jobs.Resumer interface.
func (z *resumer) Resume(ctx context.Context, _ interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
		// TODO(arul): This doesn't do anything yet.
	}
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (z *resumer) OnFailOrCancel(ctx context.Context, _ interface{}) error {
	return errors.AssertionFailedf("zone config reconciliation job can never fail or be cancelled")
}
