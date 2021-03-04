// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scjob

import (
	"context"
	"strings"

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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/errors"
)

func init() {
	jobs.RegisterConstructor(jobspb.TypeNewSchemaChange, func(
		job *jobs.Job, settings *cluster.Settings,
	) jobs.Resumer {
		pl := job.Payload()
		return &newSchemaChangeResumer{
			job:     job,
			targets: pl.GetNewSchemaChange().Targets,
		}
	})
}

type newSchemaChangeResumer struct {
	job     *jobs.Job
	targets []*scpb.Target
}

type badJobTracker struct {
	txn         *kv.Txn
	descriptors *descs.Collection
	codec       keys.SQLCodec
}

func (b badJobTracker) GetResumeSpans(
	ctx context.Context, tableID descpb.ID, indexID descpb.IndexID,
) ([]roachpb.Span, error) {
	table, err := b.descriptors.GetImmutableTableByID(ctx, b.txn, tableID, tree.ObjectLookupFlags{
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

func (n *newSchemaChangeResumer) Resume(ctx context.Context, execCtxI interface{}) (err error) {
	execCtx := execCtxI.(sql.JobExecContext)
	if err := n.job.Update(ctx, nil /* txn */, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
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
	sc, err := scplan.MakePlan(makeTargetStates(ctx, settings, n.targets, states), scplan.Params{
		ExecutionPhase: scplan.PostCommitPhase,
	})
	if err != nil {
		return err
	}
	return executeStages(ctx, n.job, execCtx, &sc, false /* reverting */)
}

func (n *newSchemaChangeResumer) OnFailOrCancel(ctx context.Context, execCtxI interface{}) error {
	execCtx := execCtxI.(sql.JobExecContext)
	if err := n.job.Update(ctx, nil /* txn */, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		return nil
	}); err != nil {
		return err
	}
	// TODO(ajwerner): Wait for leases on all descriptors before starting to
	// avoid restarts.

	progress := n.job.Progress()
	states := progress.GetNewSchemaChange().States

	settings := execCtx.ExtendedEvalContext().Settings

	// Fail if the job has entered OnFailOrCancel in a non-revertible state, which
	// should never happen during normal operation. This is indicated by the job
	// being marked noncancelable.
	if n.job.Payload().Noncancelable {
		log.Errorf(ctx, "schema change entered OnFailOrCancel in non-revertible state")
		// Attempt to clean up the job IDs from the descriptor. This has the effect
		// of unblocking all subsequent jobs. It's debatable whether this is the
		// right thing to do.
		lm := execCtx.LeaseMgr()
		db := lm.DB()
		ie := execCtx.ExtendedEvalContext().InternalExecutor.(sqlutil.InternalExecutor)
		if err := descs.Txn(ctx, settings, lm, ie, db, func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			return scexec.UpdateDescriptorJobIDs(
				ctx, txn, descriptors, n.job.Payload().DescriptorIDs, n.job.ID(), jobspb.InvalidJobID,
			)
		}); err != nil {
			return errors.Wrap(err, "attempting to clean up job IDs from descriptors")
		}
		return errors.New("schema change has entered state that is not revertible")
	}

	// Reverse the targets, and plan the reversed schema change starting at the
	// current state indicated by the nodes.
	reversed := scplan.ReverseTargetDirections(n.targets)
	sc, err := scplan.MakePlan(makeTargetStates(ctx, settings, reversed, states), scplan.Params{
		ExecutionPhase: scplan.PostCommitPhase,
	})
	if err != nil {
		return err
	}
	return executeStages(ctx, n.job, execCtx, &sc, true /* reverting */)
}

func executeStages(
	ctx context.Context, job *jobs.Job, execCtx sql.JobExecContext, sc *scplan.Plan, reverting bool,
) error {
	settings := execCtx.ExtendedEvalContext().Settings
	lm := execCtx.LeaseMgr()
	db := lm.DB()
	ie := execCtx.ExtendedEvalContext().InternalExecutor.(sqlutil.InternalExecutor)

	stmts := getStmtsForTestingMetadata(job)

	for i, s := range sc.Stages {
		var descriptorsWithUpdatedVersions []lease.IDVersion
		if err := descs.Txn(ctx, settings, lm, ie, db, func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			jt := badJobTracker{
				txn:         txn,
				descriptors: descriptors,
				codec:       execCtx.ExecCfg().Codec,
			}
			if err := scexec.NewExecutor(
				txn, descriptors, execCtx.ExecCfg().Codec, execCtx.ExecCfg().IndexBackfiller,
				jt, execCtx.ExecCfg().NewSchemaChangerTestingKnobs,
			).ExecuteOps(ctx, s.Ops, scexec.TestingKnobMetadata{
				Statements: stmts,
				Phase:      scplan.PostCommitPhase,
				Reverting:  reverting,
			}); err != nil {
				return err
			}
			// If this is the last stage, also update all the table descriptors to
			// remove the job ID.
			if i == len(sc.Stages)-1 {
				if err := scexec.UpdateDescriptorJobIDs(
					ctx, txn, descriptors, job.Payload().DescriptorIDs, job.ID(), jobspb.InvalidJobID,
				); err != nil {
					return err
				}
			}
			descriptorsWithUpdatedVersions = descriptors.GetDescriptorsWithNewVersion()
			return job.Update(ctx, txn, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
				pg := md.Progress.GetNewSchemaChange()
				pg.States = makeStates(s.After)
				ju.UpdateProgress(md.Progress)

				// Only relevant for Resume. If the resulting states are not revertible,
				// mark the job as non-cancelable. This requires updating the payload in
				// the middle of the job, which is not ideal, but it works because the
				// registry always transactionally reads the job payload when attempting
				// to cancel.
				payload := md.Payload
				if !reverting && s.NonRevertible && !payload.Noncancelable {
					payload.Noncancelable = true
					ju.UpdatePayload(md.Payload)
				}
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

func makeStates(next []*scpb.Node) []scpb.State {
	states := make([]scpb.State, len(next))
	for i := range next {
		states[i] = next[i].State
	}
	return states
}

func makeTargetStates(
	ctx context.Context, sv *cluster.Settings, protos []*scpb.Target, states []scpb.State,
) []*scpb.Node {
	if len(protos) != len(states) {
		logcrash.ReportOrPanic(ctx, &sv.SV, "unexpected slice size mismatch %d and %d",
			len(protos), len(states))
	}
	ts := make([]*scpb.Node, len(protos))
	for i := range protos {
		ts[i] = &scpb.Node{
			Target: protos[i],
			State:  states[i],
		}
	}
	return ts
}

func getStmtsForTestingMetadata(j *jobs.Job) []string {
	// This is a hack to get individual statements back out of the concatenated
	// statements.
	return strings.Split(j.Payload().Statement, "; ")
}
