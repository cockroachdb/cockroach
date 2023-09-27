package sql

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type mvccStatisticsUpdateJob struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*mvccStatisticsUpdateJob)(nil)

func (j *mvccStatisticsUpdateJob) Resume(
	ctx context.Context, execCtxI interface{},
) (jobErr error) {

	// TODO(zachlite):
	// Delete samples older than configurable setting...
	// Collect span stats for tenant descriptors...
	// Write new samples...

	return nil
}

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

func (j *mvccStatisticsUpdateJob) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeMVCCStatisticsUpdate,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &mvccStatisticsUpdateJob{job: job}
		},
		jobs.DisablesTenantCostControl,
	)
}
