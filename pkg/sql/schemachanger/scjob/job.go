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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
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
	lm := execCtx.LeaseMgr()
	db := lm.DB()
	ie := execCtx.ExtendedEvalContext().InternalExecutor.(sqlutil.InternalExecutor)
	sc, err := scplan.MakePlan(makeState(ctx, settings, n.targets, states), scplan.Params{
		ExecutionPhase: scplan.PostCommitPhase,
	})
	if err != nil {
		return err
	}
	restoreTableIDs := func(txn *kv.Txn, descriptors *descs.Collection) error {
		return scexec.UpdateDescriptorJobIDs(
			ctx, txn, descriptors, n.job.Payload().DescriptorIDs, n.job.ID(), jobspb.InvalidJobID,
		)
	}

	for i, s := range sc.Stages {
		if err := descs.Txn(ctx, settings, lm, ie, db, func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			jt := badJobTracker{
				txn:         txn,
				descriptors: descriptors,
				codec:       execCtx.ExecCfg().Codec,
			}
			if err := scexec.NewExecutor(
				txn, descriptors, execCtx.ExecCfg().Codec, execCtx.ExecCfg().IndexBackfiller,
				jt, execCtx.ExecCfg().NewSchemaChangerTestingKnobs, execCtx.ExecCfg().JobRegistry,
				execCtx.ExecCfg().InternalExecutor).ExecuteOps(ctx, s.Ops, scexec.TestingKnobMetadata{
				Statements: n.job.Payload().Statement,
				Phase:      scplan.PostCommitPhase,
			}); err != nil {
				return err
			}
			// If this is the last stage, also update all the table descriptors to
			// remove the job ID.
			if i == len(sc.Stages)-1 {
				if err := restoreTableIDs(txn, descriptors); err != nil {
					return err
				}
			}
			return n.job.Update(ctx, txn, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
				pg := md.Progress.GetNewSchemaChange()
				pg.States = makeStatuses(s.After)
				ju.UpdateProgress(md.Progress)
				return nil
			})
		}); err != nil {
			return err
		}
		err := execCtx.ExecCfg().JobRegistry.NotifyToAdoptJobs(ctx)
		if err != nil {
			return err
		}
	}

	// If no stages exist, then execute a singe transaction
	// within this job to allow schema changes again.
	if len(sc.Stages) == 0 {
		err := descs.Txn(ctx, settings, lm, ie, db, func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			err := restoreTableIDs(txn, descriptors)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		err = execCtx.ExecCfg().JobRegistry.NotifyToAdoptJobs(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func makeStatuses(next scpb.State) []scpb.Status {
	states := make([]scpb.Status, len(next))
	for i := range next {
		states[i] = next[i].Status
	}
	return states
}

func makeState(
	ctx context.Context, sv *cluster.Settings, protos []*scpb.Target, states []scpb.Status,
) scpb.State {
	if len(protos) != len(states) {
		logcrash.ReportOrPanic(ctx, &sv.SV, "unexpected slice size mismatch %d and %d",
			len(protos), len(states))
	}
	ts := make(scpb.State, len(protos))
	for i := range protos {
		ts[i] = &scpb.Node{
			Target: protos[i],
			Status: states[i],
		}
	}
	return ts
}

func (n *newSchemaChangeResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	panic("unimplemented")
}
