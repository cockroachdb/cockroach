// Copyright 2017 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type alterIndexNode struct {
	n         *tree.AlterIndex
	tableDesc *tabledesc.Mutable
	indexDesc *descpb.IndexDescriptor
}

// AlterIndex applies a schema change on an index.
// Privileges: CREATE on table.
func (p *planner) AlterIndex(ctx context.Context, n *tree.AlterIndex) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER INDEX",
	); err != nil {
		return nil, err
	}

	tableDesc, indexDesc, err := p.getTableAndIndex(ctx, &n.Index, privilege.CREATE)
	if err != nil {
		return nil, err
	}
	// As an artifact of finding the index by name, we get a pointer to a
	// different copy than the one in the tableDesc. To make it easier for the
	// code below, get a pointer to the index descriptor that's actually in
	// tableDesc.
	index, err := tableDesc.FindIndexWithID(indexDesc.ID)
	if err != nil {
		return nil, err
	}
	return &alterIndexNode{n: n, tableDesc: tableDesc, indexDesc: index.IndexDesc()}, nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because ALTER INDEX performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *alterIndexNode) ReadingOwnWrites() {}

func (n *alterIndexNode) startExec(params runParams) error {
	// Commands can either change the descriptor directly (for
	// alterations that don't require a backfill) or add a mutation to
	// the list.
	descriptorChanged := false
	origNumMutations := len(n.tableDesc.Mutations)

	for _, cmd := range n.n.Cmds {
		switch t := cmd.(type) {
		case *tree.AlterIndexPartitionBy:
			telemetry.Inc(sqltelemetry.SchemaChangeAlterCounterWithExtra("index", "partition_by"))
			if n.tableDesc.GetLocalityConfig() != nil {
				return pgerror.Newf(
					pgcode.FeatureNotSupported,
					"cannot change the partitioning of an index if the table is part of a multi-region database",
				)
			}
			if n.tableDesc.PartitionAllBy {
				return pgerror.Newf(
					pgcode.FeatureNotSupported,
					"cannot change the partitioning of an index if the table has PARTITION ALL BY defined",
				)
			}
			if n.indexDesc.Partitioning.NumImplicitColumns > 0 {
				return unimplemented.New(
					"ALTER INDEX PARTITION BY",
					"cannot ALTER INDEX PARTITION BY on an index which already has implicit column partitioning",
				)
			}
			allowImplicitPartitioning := params.p.EvalContext().SessionData.ImplicitColumnPartitioningEnabled ||
				n.tableDesc.IsLocalityRegionalByRow()
			newIndexDesc, err := CreatePartitioning(
				params.ctx,
				params.extendedEvalCtx.Settings,
				params.EvalContext(),
				n.tableDesc,
				*n.indexDesc,
				t.PartitionBy,
				nil, /* allowedNewColumnNames */
				allowImplicitPartitioning,
			)
			if err != nil {
				return err
			}
			if newIndexDesc.Partitioning.NumImplicitColumns > 0 {
				return unimplemented.New(
					"ALTER INDEX PARTITION BY",
					"cannot ALTER INDEX and change the partitioning to contain implicit columns",
				)
			}
			descriptorChanged = !n.indexDesc.Equal(&newIndexDesc)
			if err = deleteRemovedPartitionZoneConfigs(
				params.ctx,
				params.p.txn,
				n.tableDesc,
				n.indexDesc,
				&n.indexDesc.Partitioning,
				&newIndexDesc.Partitioning,
				params.extendedEvalCtx.ExecCfg,
			); err != nil {
				return err
			}
			*n.indexDesc = newIndexDesc
		default:
			return errors.AssertionFailedf(
				"unsupported alter command: %T", cmd)
		}
	}

	if err := n.tableDesc.AllocateIDs(params.ctx); err != nil {
		return err
	}

	addedMutations := len(n.tableDesc.Mutations) > origNumMutations
	if !addedMutations && !descriptorChanged {
		// Nothing to be done
		return nil
	}
	mutationID := descpb.InvalidMutationID
	if addedMutations {
		mutationID = n.tableDesc.ClusterVersion.NextMutationID
	}
	if err := params.p.writeSchemaChange(
		params.ctx, n.tableDesc, mutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Record this index alteration in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	return params.p.logEvent(params.ctx,
		n.tableDesc.ID,
		&eventpb.AlterIndex{
			TableName:  n.n.Index.Table.FQString(),
			IndexName:  n.indexDesc.Name,
			MutationID: uint32(mutationID),
		})
}

func (n *alterIndexNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterIndexNode) Close(context.Context)        {}
