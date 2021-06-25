package job

import (
	"context"

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
	reconciliationMgr := execCtx.ReconciliationMgr()

	// TODO(zcfgs-pod): Listen in on rangefeeds over system.{descriptor,zones}
	// and construct the right update, instead of this placeholder write.
	nameSpaceTableStart := execCtx.ExecCfg().Codec.TablePrefix(keys.NamespaceTableID)
	nameSpaceTableSpan := roachpb.Span{
		Key:    nameSpaceTableStart,
		EndKey: nameSpaceTableStart.PrefixEnd(),
	}
	log.Info(ctx, "xxx: upserting some random span entry")
	var upsert, delete []roachpb.SpanConfigEntry
	upsert = append(upsert, roachpb.SpanConfigEntry{
		Span: nameSpaceTableSpan,
		Config: roachpb.SpanConfig{
			NumReplicas: 42,
		},
	})

	// XXX: wiring up to tenant connector directly is no applicable for the
	// system tenant.
	return reconciliationMgr.SC.UpdateSpanConfigEntries(ctx, upsert, delete)
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (z *resumer) OnFailOrCancel(ctx context.Context, _ interface{}) error {
	return errors.AssertionFailedf("zone config reconciliation job can never fail or be cancelled")
}
