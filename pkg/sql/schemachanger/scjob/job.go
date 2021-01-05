package scjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
)

func init() {
	jobs.RegisterConstructor(jobspb.TypeNewSchemaChange, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		pl := job.Payload()
		return &newSchemaChangeResumer{
			job:     job,
			targets: pl.GetNewSchemaChange().Targets,
		}
	})
}

type newSchemaChangeResumer struct {
	job     *jobs.Job
	targets []*scpb.TargetProto
}

type badJobTracker struct {
	txn         *kv.Txn
	descriptors *descs.Collection
	codec       keys.SQLCodec
}

func (b badJobTracker) GetResumeSpans(
	ctx context.Context, tableID descpb.ID, indexID descpb.IndexID,
) ([]roachpb.Span, error) {
	table, err := b.descriptors.GetTableVersionByID(ctx, b.txn, tableID, tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{
			Required:    true,
			AvoidCached: true,
		},
	})
	if err != nil {
		return nil, err
	}
	return []roachpb.Span{table.IndexSpan(b.codec, indexID)}, nil
}

func (b badJobTracker) SetResumeSpans(
	ctx context.Context, tableID descpb.ID, indexID descpb.IndexID, total, done []roachpb.Span,
) error {
	panic("implement me")
}

var _ scexec.JobProgressTracker = (*badJobTracker)(nil)

func (n *newSchemaChangeResumer) Resume(
	ctx context.Context, execCtxI interface{}, resultsCh chan<- tree.Datums,
) (err error) {
	execCtx := execCtxI.(sql.JobExecContext)
	if err := n.job.WithTxn(nil).Update(ctx, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		return nil
	}); err != nil {
		// TODO(ajwerner): Detect transient errors and classify as retriable here or
		// in the jobs package.
		return err
	}
	// TODO(ajwerner): Wait for leases on all descriptors before starting to
	// avoid restarts.

	progress := n.job.Progress()
	states := progress.GetNewSchemaChange().States

	settings := execCtx.ExtendedEvalContext().Settings
	lm := execCtx.LeaseMgr()
	db := lm.DB()
	ie := execCtx.ExtendedEvalContext().InternalExecutor.(sqlutil.InternalExecutor)
	sc, err := scplan.Compile(makeTargetStates(ctx, settings, n.targets, states), scplan.CompileFlags{
		ExecutionPhase: scplan.PostCommitPhase,
	})
	if err != nil {
		return err
	}

	for _, s := range sc.Stages() {
		var descriptorsWithUpdatedVersions []lease.IDVersion
		if err := descs.Txn(ctx, settings, lm, ie, db, func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			jt := badJobTracker{
				txn:         txn,
				descriptors: descriptors,
				codec:       execCtx.ExecCfg().Codec,
			}
			if err := scexec.New(txn, descriptors, execCtx.ExecCfg().Codec, execCtx.ExecCfg().IndexBackfiller, jt).ExecuteOps(ctx, s.Ops); err != nil {
				return err
			}
			descriptorsWithUpdatedVersions = descriptors.GetDescriptorsWithNewVersion()
			defer n.job.WithTxn(nil)
			return n.job.WithTxn(txn).Update(ctx, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
				pg := md.Progress.GetNewSchemaChange()
				pg.States = makeStates(s.NextTargets)
				ju.UpdateProgress(md.Progress)
				return nil
			})
		}); err != nil {
			return err
		}

		// Wait for new versions.
		if err := sql.WaitToUpdateLeasesMultiple(
			ctx,
			lm,
			descriptorsWithUpdatedVersions,
		); err != nil {
			return err
		}
	}
	return nil
}

func makeStates(nextTargets []scpb.TargetState) []scpb.State {
	states := make([]scpb.State, len(nextTargets))
	for i := range nextTargets {
		states[i] = nextTargets[i].State
	}
	return states
}

func makeTargetStates(
	ctx context.Context, sv *cluster.Settings, protos []*scpb.TargetProto, states []scpb.State,
) []scpb.TargetState {
	if len(protos) != len(states) {
		logcrash.ReportOrPanic(ctx, &sv.SV, "unexpected slice size mismatch %d and %d",
			len(protos), len(states))
	}
	ts := make([]scpb.TargetState, len(protos))
	for i := range protos {
		ts[i] = scpb.TargetState{
			Target: protos[i].GetValue().(scpb.Target),
			State:  states[i],
		}
	}
	return ts
}

func (n *newSchemaChangeResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	panic("unimplemented")
}

type ProgressTracker struct {
}
