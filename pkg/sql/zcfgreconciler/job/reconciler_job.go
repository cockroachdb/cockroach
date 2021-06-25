package job

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
func (z *resumer) Resume(ctx context.Context, execCtxI interface{}) error {
	execCtx := execCtxI.(sql.JobExecContext)

	// TODO(zcfgs-pod): Listen in on rangefeeds over system.{descriptor,zones}
	// and construct the right update, instead of this placeholder write over
	// the same keyspan over and over again.
	nameSpaceTableStart := execCtx.ExecCfg().Codec.TablePrefix(keys.NamespaceTableID)
	nameSpaceTableSpan := roachpb.Span{
		Key:    nameSpaceTableStart,
		EndKey: nameSpaceTableStart.PrefixEnd(),
	}

	for {
		var update []roachpb.SpanConfigEntry
		var delete []roachpb.Span
		update = append(update, roachpb.SpanConfigEntry{
			Span:   nameSpaceTableSpan,
			Config: roachpb.SpanConfig{},
		})
		rc := execCtx.ConfigReconciliationJobDeps()

		// NB: We cannot return from this method, as that would put the job in
		// an un-revertable, non-running state.
		if err := rc.UpdateSpanConfigEntries(ctx, update, delete); err != nil {
			log.Errorf(ctx, "config reconciliation error: %v", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			time.Sleep(5 * time.Second)
		}
	}
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (z *resumer) OnFailOrCancel(context.Context, interface{}) error {
	return errors.AssertionFailedf("zone config reconciliation job can never fail or be cancelled")
}
