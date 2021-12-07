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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scsqldeps"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// SchemaChange provides the planNode for the new schema changer.
func (p *planner) SchemaChange(ctx context.Context, stmt tree.Statement) (planNode, bool, error) {
	// TODO(ajwerner): Call featureflag.CheckEnabled appropriately.
	mode := p.extendedEvalCtx.SchemaChangerState.mode
	// When new schema changer is on we will not support it for explicit
	// transaction, since we don't know if subsequent statements don't
	// support it.
	if mode == sessiondatapb.UseNewSchemaChangerOff ||
		((mode == sessiondatapb.UseNewSchemaChangerOn ||
			mode == sessiondatapb.UseNewSchemaChangerUnsafe) && !p.extendedEvalCtx.TxnImplicit) {
		return nil, false, nil
	}
	scs := p.extendedEvalCtx.SchemaChangerState
	scs.stmts = append(scs.stmts, p.stmt.SQL)
	deps := scdeps.NewBuilderDependencies(
		p.ExecCfg().Codec,
		p.Txn(),
		p.Descriptors(),
		p,
		p,
		p.SessionData(),
		p.ExecCfg().Settings,
		scs.stmts,
	)
	outputNodes, err := scbuild.Build(ctx, deps, scs.state, stmt)
	if scerrors.HasNotImplemented(err) &&
		mode != sessiondatapb.UseNewSchemaChangerUnsafeAlways {
		return nil, false, nil
	}
	if err != nil {
		// If we need to wait for a concurrent schema change to finish, release our
		// leases, and then return the error to wait and retry.
		if scerrors.ConcurrentSchemaChangeDescID(err) != descpb.InvalidID {
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

	if knobs := p.ExecCfg().DeclarativeSchemaChangerTestingKnobs; knobs != nil &&
		knobs.BeforeWaitingForConcurrentSchemaChanges != nil {
		knobs.BeforeWaitingForConcurrentSchemaChanges(scs.stmts)
	}

	everySecond := log.Every(time.Second)
	ie := p.ExecCfg().InternalExecutorFactory(ctx, p.SessionData())
	defer ie.Close(ctx)
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		now := p.ExecCfg().Clock.Now()
		if everySecond.ShouldLog() {
			log.Infof(ctx, "schema change waiting for concurrent schema changes on descriptor %d", descID)
		}
		blocked := false
		if err := p.ExecCfg().CollectionFactory.Txn(
			ctx, ie, p.ExecCfg().DB,
			func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
				if err := txn.SetFixedTimestamp(ctx, now); err != nil {
					return err
				}
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
				blocked = catalog.HasConcurrentSchemaChanges(table)
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
	scs := p.ExtendedEvalContext().SchemaChangerState
	runDeps := newSchemaChangerTxnRunDependencies(
		p.ExecCfg(), p.Txn(), p.Descriptors(), p.EvalContext(),
		scs, scop.StatementPhase,
	)
	after, err := scrun.RunSchemaChangesInTxn(params.ctx, runDeps, s.plannedState)
	if err != nil {
		return err
	}
	scs.state = after
	return nil
}

func newSchemaChangerTxnRunDependencies(
	execCfg *ExecutorConfig,
	txn *kv.Txn,
	descriptors *descs.Collection,
	evalContext *tree.EvalContext,
	scs *SchemaChangerState,
	phase scop.Phase,
) scrun.TxnRunDependencies {
	execDeps := scdeps.NewExecutorDependencies(
		execCfg.Codec,
		txn,
		descriptors,
		execCfg.JobRegistry,
		execCfg.IndexBackfiller,
		execCfg.IndexValidator,
		scsqldeps.NewCCLCallbacks(execCfg.Settings, evalContext),
		func(ctx context.Context, txn *kv.Txn, depth int, descID descpb.ID, metadata scpb.ElementMetadata, event eventpb.EventPayload) error {
			return LogEventForSchemaChanger(ctx, execCfg, txn, depth+1, descID, metadata, event)
		},
		scs.stmts,
	)
	runDeps := scdeps.NewTxnRunDependencies(
		execDeps, phase, execCfg.DeclarativeSchemaChangerTestingKnobs,
	)
	return runDeps
}

func (s schemaChangePlanNode) Next(params runParams) (bool, error) { return false, nil }
func (s schemaChangePlanNode) Values() tree.Datums                 { return tree.Datums{} }
func (s schemaChangePlanNode) Close(ctx context.Context)           {}

var _ (planNode) = (*schemaChangePlanNode)(nil)
