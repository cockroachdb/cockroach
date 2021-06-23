package zcfgreconciler

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/errors"
)

// zcfgReconciliationJobID is the static ID of the ZcfgReconciliationJob. This
// is used to ensure the job remains a singleton.
const zcfgReconciliationJobID jobspb.JobID = 1

// zcfgReconciliationResumer is the anchor struct for the zcfg reconciliation
// job.
type zcfgReconciliationResumer struct {
	job *jobs.Job
}

// Resume implements the jobs.Resumer interface.
func (z *zcfgReconciliationResumer) Resume(ctx context.Context, _ interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
		// TODO(arul): This doesn't do anything yet.
	}
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (z *zcfgReconciliationResumer) OnFailOrCancel(ctx context.Context, _ interface{}) error {
	return errors.AssertionFailedf("zcfg reconciliation job can never fail or be cancelled")
}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &zcfgReconciliationResumer{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeZcfgReconciliation, createResumerFn)
}

type zcfgReconciliationJobExistsError struct{}

var _ error = zcfgReconciliationJobExistsError{}

func (zcfgReconciliationJobExistsError) Error() string {
	return "zcfg reconciliation job already exists"
}

// ZcfgReconciliationJobExistsError is reported when a zcfg job already exists.
// This is a sentinel error.
var ZcfgReconciliationJobExistsError error = zcfgReconciliationJobExistsError{}
