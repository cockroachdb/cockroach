package job

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/compiler"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/executor"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
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
	targets []*targets.TargetProto
}

func (n *newSchemaChangeResumer) Resume(
	ctx context.Context, execCtxI interface{}, resultsCh chan<- tree.Datums,
) error {
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
	stages, err := compiler.Compile(makeTargetStates(ctx, settings, n.targets, states), compiler.CompileFlags{
		ExecutionPhase: compiler.PostCommitPhase,
	})
	if err != nil {
		return err
	}

	for _, s := range stages {
		var descriptorsWithUpdatedVersions []lease.IDVersion
		if err := descs.Txn(ctx, settings, lm, ie, db, func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			if err := executor.New(txn, descriptors).ExecuteOps(ctx, s.Ops); err != nil {
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

func makeStates(nextTargets []targets.TargetState) []targets.State {
	states := make([]targets.State, len(nextTargets))
	for i := range nextTargets {
		states[i] = nextTargets[i].State
	}
	return states
}

func makeTargetStates(
	ctx context.Context, sv *cluster.Settings, protos []*targets.TargetProto, states []targets.State,
) []targets.TargetState {
	if len(protos) != len(states) {
		logcrash.ReportOrPanic(ctx, &sv.SV, "unexpected slice size mismatch %d and %d",
			len(protos), len(states))
	}
	ts := make([]targets.TargetState, len(protos))
	for i := range protos {
		ts[i] = targets.TargetState{
			Target: protos[i].GetValue().(targets.Target),
			State:  states[i],
		}
	}
	return ts
}

func (n *newSchemaChangeResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	panic("unimplemented")
}
