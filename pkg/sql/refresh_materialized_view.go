// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

type refreshMaterializedViewNode struct {
	zeroInputPlanNode
	n *tree.RefreshMaterializedView
}

func (p *planner) RefreshMaterializedView(
	ctx context.Context, n *tree.RefreshMaterializedView,
) (planNode, error) {
	_, desc, err := p.ResolveMutableTableDescriptorEx(ctx, n.Name, true /* required */, tree.ResolveRequireViewDesc)
	if err != nil {
		return nil, err
	}
	if !desc.MaterializedView() {
		return nil, pgerror.Newf(pgcode.WrongObjectType, "%q is not a materialized view", desc.Name)
	}

	hasOwnership, err := p.HasOwnership(ctx, desc)
	if err != nil {
		return nil, err
	}

	if !hasOwnership {
		return nil, pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"must be owner of materialized view %s",
			desc.Name,
		)
	}

	return &refreshMaterializedViewNode{n: n}, nil
}

func (n *refreshMaterializedViewNode) StartExec(params runParams) error {
	// We refresh a materialized view by creating a new set of indexes to write
	// the result of the view query into. The existing set of indexes will remain
	// present and readable so that reads of the view during the refresh operation
	// will return consistent data. The schema change process will backfill the
	// results of the view query into the new set of indexes, and then change the
	// set of indexes over to the new set of indexes atomically.

	if !params.P.(*planner).extendedEvalCtx.TxnIsSingleStmt() {
		return pgerror.Newf(pgcode.InvalidTransactionState, "cannot refresh view in a multi-statement transaction")
	}

	telemetry.Inc(sqltelemetry.SchemaRefreshMaterializedView)

	// Inform the user that CONCURRENTLY is not needed.
	if n.n.Concurrently {
		params.P.(*planner).BufferClientNotice(
			params.Ctx,
			pgnotice.Newf("CONCURRENTLY is not required as views are refreshed concurrently"),
		)
	}

	_, desc, err := params.P.(*planner).ResolveMutableTableDescriptorEx(params.Ctx, n.n.Name, true /* required */, tree.ResolveRequireViewDesc)
	if err != nil {
		return err
	}

	// TODO (rohany): Not sure if this is a real restriction, but let's start with
	//  it to be safe.
	for i := range desc.Mutations {
		mut := &desc.Mutations[i]
		if mut.GetMaterializedViewRefresh() != nil {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState, "view is already being refreshed")
		}
	}

	// Prepare the new set of indexes by cloning all existing indexes on the view.
	newPrimaryIndex := desc.GetPrimaryIndex().IndexDescDeepCopy()
	newIndexes := make([]descpb.IndexDescriptor, len(desc.PublicNonPrimaryIndexes()))
	for i, idx := range desc.PublicNonPrimaryIndexes() {
		newIndexes[i] = idx.IndexDescDeepCopy()
	}

	// Reset and allocate new IDs for the new indexes.
	getID := func() descpb.IndexID {
		res := desc.NextIndexID
		desc.NextIndexID++
		return res
	}
	newPrimaryIndex.ID = getID()
	for i := range newIndexes {
		newIndexes[i].ID = getID()
	}

	// Set RefreshViewRequired to false. This will allow SELECT operations on the materialized
	// view to succeed when the view has been created with the NO DATA option.
	desc.RefreshViewRequired = false
	// Queue the refresh mutation.
	refreshProto := &descpb.MaterializedViewRefresh{
		NewPrimaryIndex: newPrimaryIndex,
		NewIndexes:      newIndexes,
		AsOf:            params.P.(*planner).Txn().ReadTimestamp(),
		ShouldBackfill:  n.n.RefreshDataOption != tree.RefreshDataClear,
	}
	if asOf := params.P.(*planner).EvalContext().AsOfSystemTime; asOf != nil && asOf.ForBackfill {
		refreshProto.AsOf = params.P.(*planner).EvalContext().AsOfSystemTime.Timestamp
	} else {
		refreshProto.AsOf = params.P.(*planner).Txn().ReadTimestamp()
	}
	desc.AddMaterializedViewRefreshMutation(refreshProto)

	// Log the refresh materialized view event.
	if err := params.P.(*planner).logEvent(params.Ctx,
		desc.ID,
		&eventpb.RefreshMaterializedView{
			ViewName: params.P.(*planner).ResolvedName(n.n.Name).FQString(),
		}); err != nil {
		return err
	}

	return params.P.(*planner).writeSchemaChange(
		params.Ctx,
		desc,
		desc.ClusterVersion().NextMutationID,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	)
}

func (n *refreshMaterializedViewNode) Next(params runParams) (bool, error) { return false, nil }
func (n *refreshMaterializedViewNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *refreshMaterializedViewNode) Close(ctx context.Context)           {}
func (n *refreshMaterializedViewNode) ReadingOwnWrites()                   {}
