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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

type alterIndexNode struct {
	n         *tree.AlterIndex
	tableDesc *sqlbase.MutableTableDescriptor
	indexDesc *sqlbase.IndexDescriptor
}

// AlterIndex applies a schema change on an index.
// Privileges: CREATE on table.
func (p *planner) AlterIndex(ctx context.Context, n *tree.AlterIndex) (planNode, error) {
	tableDesc, indexDesc, err := p.getTableAndIndex(ctx, &n.Index, privilege.CREATE)
	if err != nil {
		return nil, err
	}
	// As an artifact of finding the index by name, we get a pointer to a
	// different copy than the one in the tableDesc. To make it easier for the
	// code below, get a pointer to the index descriptor that's actually in
	// tableDesc.
	indexDesc, err = tableDesc.FindIndexByID(indexDesc.ID)
	if err != nil {
		return nil, err
	}
	return &alterIndexNode{n: n, tableDesc: tableDesc, indexDesc: indexDesc}, nil
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
			partitioning, err := CreatePartitioning(
				params.ctx, params.extendedEvalCtx.Settings,
				params.EvalContext(),
				n.tableDesc, n.indexDesc, t.PartitionBy)
			if err != nil {
				return err
			}
			descriptorChanged = !proto.Equal(
				&n.indexDesc.Partitioning,
				&partitioning,
			)
			err = deleteRemovedPartitionZoneConfigs(
				params.ctx, params.p.txn,
				n.tableDesc.TableDesc(), n.indexDesc,
				&n.indexDesc.Partitioning, &partitioning,
				params.extendedEvalCtx.ExecCfg,
			)
			if err != nil {
				return err
			}
			n.indexDesc.Partitioning = partitioning
		default:
			return errors.AssertionFailedf(
				"unsupported alter command: %T", cmd)
		}
	}

	if err := n.tableDesc.AllocateIDs(); err != nil {
		return err
	}

	addedMutations := len(n.tableDesc.Mutations) > origNumMutations
	if !addedMutations && !descriptorChanged {
		// Nothing to be done
		return nil
	}
	mutationID := sqlbase.InvalidMutationID
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
	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogAlterIndex,
		int32(n.tableDesc.ID),
		int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
		struct {
			TableName  string
			IndexName  string
			Statement  string
			User       string
			MutationID uint32
		}{
			n.n.Index.Table.FQString(), n.indexDesc.Name, n.n.String(),
			params.SessionData().User, uint32(mutationID),
		},
	)
}

func (n *alterIndexNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterIndexNode) Close(context.Context)        {}
