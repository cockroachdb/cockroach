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

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// SchemaChange provides the planNode for the new schema changer.
func (p *planner) SchemaChange(ctx context.Context, stmt tree.Statement) (planNode, bool, error) {
	// TODO(ajwerner): Call featureflag.CheckEnabled appropriately.
	mode := p.extendedEvalCtx.SchemaChangerState.mode
	if mode == sessiondata.UseNewSchemaChangerOff ||
		(mode == sessiondata.UseNewSchemaChangerOn && !p.extendedEvalCtx.TxnImplicit) {
		return nil, false, nil
	}
	b := scbuild.NewBuilder(p, p.SemaCtx(), p.EvalContext())
	updated, err := b.Build(ctx, p.extendedEvalCtx.SchemaChangerState.nodes, stmt)
	if scbuild.HasNotImplemented(err) && mode == sessiondata.UseNewSchemaChangerOn {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &schemaChangePlanNode{
		plannedState: updated,
	}, true, nil
}

// schemaChangePlanNode is the planNode utilized by the new schema changer to
//
type schemaChangePlanNode struct {
	// plannedState contains the set of states produced by the builder combining
	// the nodes that existed preceding the current statement with the output of
	// the built current statement.
	//
	// TODO(ajwerner): Give this a better name.
	plannedState []*scpb.Node
}

func (s *schemaChangePlanNode) startExec(params runParams) error {
	executor := scexec.NewExecutor(params.p.txn, params.p.Descriptors(), params.p.EvalContext().Codec,
		nil /* backfiller */, nil /* jobTracker */)
	after, err := runNewSchemaChanger(
		params.ctx, scplan.StatementPhase, s.plannedState, executor,
	)
	if err != nil {
		return err
	}
	scs := params.p.extendedEvalCtx.SchemaChangerState
	scs.nodes = after
	return nil
}

func (s schemaChangePlanNode) Next(params runParams) (bool, error) { return false, nil }
func (s schemaChangePlanNode) Values() tree.Datums                 { return tree.Datums{} }
func (s schemaChangePlanNode) Close(ctx context.Context)           {}

var _ (planNode) = (*schemaChangePlanNode)(nil)
