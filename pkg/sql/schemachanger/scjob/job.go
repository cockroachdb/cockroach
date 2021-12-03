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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scsqldeps"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

func init() {
	jobs.RegisterConstructor(jobspb.TypeNewSchemaChange, func(
		job *jobs.Job, settings *cluster.Settings,
	) jobs.Resumer {
		return &newSchemaChangeResumer{
			job: job,
		}
	})
}

type newSchemaChangeResumer struct {
	job      *jobs.Job
	rollback bool
}

func (n *newSchemaChangeResumer) Resume(ctx context.Context, execCtxI interface{}) (err error) {
	return n.run(ctx, execCtxI)
}

func (n *newSchemaChangeResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	n.rollback = true
	return n.run(ctx, execCtx)
}

func (n *newSchemaChangeResumer) run(ctx context.Context, execCtxI interface{}) error {
	execCtx := execCtxI.(sql.JobExecContext)
	execCfg := execCtx.ExecCfg()
	if err := n.job.Update(ctx, nil /* txn */, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		return nil
	}); err != nil {
		// TODO(ajwerner): Detect transient errors and classify as retriable here or
		// in the jobs package.
		return err
	}
	// TODO(ajwerner): Wait for leases on all descriptors before starting to
	// avoid restarts.

	payload := n.job.Payload()
	progress := n.job.Progress()
	newSchemaChangeProgress := progress.GetNewSchemaChange()
	newSchemaChangeDetails := payload.GetNewSchemaChange()
	deps := scdeps.NewJobRunDependencies(
		execCfg.CollectionFactory,
		execCfg.DB,
		execCfg.InternalExecutor,
		execCfg.IndexBackfiller,
		func(ctx context.Context, txn *kv.Txn, depth int, descID descpb.ID, metadata scpb.ElementMetadata, event eventpb.EventPayload) error {
			return sql.LogEventForSchemaChanger(ctx, execCtx.ExecCfg(), txn, depth, descID, metadata, event)
		},
		execCfg.JobRegistry,
		n.job,
		execCfg.Codec,
		execCfg.Settings,
		execCfg.IndexValidator,
		scsqldeps.NewCCLCallbacks(execCfg.Settings, nil),
		execCfg.DeclarativeSchemaChangerTestingKnobs,
		payload.Statement,
	)

	return scrun.RunSchemaChangesInJob(
		ctx, execCfg.DeclarativeSchemaChangerTestingKnobs, execCfg.Settings,
		deps, n.job.ID(), payload.DescriptorIDs,
		*newSchemaChangeDetails, *newSchemaChangeProgress,
		n.rollback,
	)
}
