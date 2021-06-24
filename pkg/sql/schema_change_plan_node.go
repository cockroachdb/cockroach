// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// SchemaChange provides the planNode for the new schema changer.
func (p *planner) SchemaChange(ctx context.Context, stmt tree.Statement) (planNode, bool, error) {
	// TODO(ajwerner): Call featureflag.CheckEnabled appropriately.
	mode := p.extendedEvalCtx.SchemaChangerState.mode
	if mode == sessiondata.UseNewSchemaChangerOff ||
		(mode == sessiondata.UseNewSchemaChangerOn && !p.extendedEvalCtx.TxnImplicit) {
		return nil, false, nil
	}
	scs := p.extendedEvalCtx.SchemaChangerState
	scs.stmts = append(scs.stmts, p.stmt.SQL)
	buildDeps := scbuild.Dependencies{
		Res:          p,
		SemaCtx:      p.SemaCtx(),
		EvalCtx:      p.EvalContext(),
		Descs:        p.Descriptors(),
		AuthAccessor: p,
	}
	outputNodes, err := scbuild.Build(ctx, buildDeps, p.extendedEvalCtx.SchemaChangerState.state, stmt)
	if scbuild.HasNotImplemented(err) && mode == sessiondata.UseNewSchemaChangerOn {
		return nil, false, nil
	}
	if err != nil {
		// If we need to wait for a concurrent schema change to finish, release our
		// leases, and then return the error to wait and retry.
		if cscErr := (*scbuild.ConcurrentSchemaChangeError)(nil); errors.As(err, &cscErr) {
			p.Descriptors().ReleaseLeases(ctx)
		}
		return nil, false, err
	}
	return &schemaChangePlanNode{
		plannedState: outputNodes,
	}, true, nil
}

// WaitForDescriptorSchemaChanges polls the specified descriptor (in separate
// transactions) until all its ongoing schema changes have completed.
func (p *planner) WaitForDescriptorSchemaChanges(
	ctx context.Context, descID descpb.ID, scs SchemaChangerState,
) error {

	if knobs := p.ExecCfg().NewSchemaChangerTestingKnobs; knobs != nil &&
		knobs.BeforeWaitingForConcurrentSchemaChanges != nil {
		knobs.BeforeWaitingForConcurrentSchemaChanges(scs.stmts)
	}

	everySecond := log.Every(time.Second)
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		now := p.ExecCfg().Clock.Now()
		if everySecond.ShouldLog() {
			log.Infof(ctx, "schema change waiting for concurrent schema changes on descriptor %d", descID)
		}
		blocked := false
		if err := descs.Txn(
			ctx, p.ExecCfg().Settings, p.LeaseMgr(), p.ExecCfg().InternalExecutor, p.ExecCfg().DB,
			func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
				txn.SetFixedTimestamp(ctx, now)
				table, err := descriptors.GetImmutableTableByID(ctx, txn, descID,
					tree.ObjectLookupFlags{
						CommonLookupFlags: tree.CommonLookupFlags{
							Required:    true,
							AvoidCached: true,
						},
					})
				if err != nil {
					return err
				}
				blocked = scbuild.HasConcurrentSchemaChanges(table)
				return nil
			}); err != nil {
			return err
		}
		if !blocked {
			break
		}
	}
	log.Infof(ctx, "done waiting for concurrent schema changes on descriptor %d", descID)
	return nil
}

// schemaChangePlanNode is the planNode utilized by the new schema changer to
// perform all schema changes, unified in the new schema changer.
type schemaChangePlanNode struct {
	// plannedState contains the set of states produced by the builder combining
	// the nodes that existed preceding the current statement with the output of
	// the built current statement.
	plannedState scpb.State
}

func (s *schemaChangePlanNode) startExec(params runParams) error {
	p := params.p
	scs := p.extendedEvalCtx.SchemaChangerState
	executor := scexec.NewExecutor(p.txn, p.Descriptors(), p.EvalContext().Codec,
		nil /* backfiller */, nil /* jobTracker */, p.ExecCfg().NewSchemaChangerTestingKnobs,
		params.extendedEvalCtx.ExecCfg.JobRegistry, params.p.execCfg.InternalExecutor)
	after, err := runNewSchemaChanger(
		params.ctx, scplan.StatementPhase, s.plannedState, executor, scs.stmts,
	)
	if err != nil {
		return err
	}
	scs.state = after
	return nil
}

func (s schemaChangePlanNode) Next(params runParams) (bool, error) { return false, nil }
func (s schemaChangePlanNode) Values() tree.Datums                 { return tree.Datums{} }
func (s schemaChangePlanNode) Close(ctx context.Context)           {}

var _ (planNode) = (*schemaChangePlanNode)(nil)
