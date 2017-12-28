// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"

	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type alterIndexNode struct {
	n         *tree.AlterIndex
	tableDesc *sqlbase.TableDescriptor
	indexDesc *sqlbase.IndexDescriptor
}

// AlterIndex applies a schema change on an index.
// Privileges: CREATE on table.
func (p *Planner) AlterIndex(ctx context.Context, n *tree.AlterIndex) (planNode, error) {
	tableDesc, indexDesc, err := p.getTableAndIndex(ctx, &n.Index.Table, n.Index, privilege.CREATE)
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

func (n *alterIndexNode) startExec(params runParams) error {
	// Commands can either change the descriptor directly (for
	// alterations that don't require a backfill) or add a mutation to
	// the list.
	descriptorChanged := false
	origNumMutations := len(n.tableDesc.Mutations)

	for _, cmd := range n.n.Cmds {
		switch t := cmd.(type) {
		case *tree.AlterIndexPartitionBy:
			previousTableDesc := *n.tableDesc
			partitioning, _, err := createPartitionedBy(
				params.ctx, params.p.evalCtx.Settings, &params.p.evalCtx, n.tableDesc, n.indexDesc, t.PartitionBy)
			if err != nil {
				return err
			}
			n.indexDesc.Partitioning = partitioning
			// TODO(dan): This checks TableDescriptors instead of
			// PartitioningDescriptors to mirror the fast path check. Of course,
			// RepartitioningFastPathAvailable could also be acting on
			// PartitioningDescriptors but then it would need a complicated
			// method signature and it felt weird to impose that on the
			// sql<->sqlccl hook. Revisit?
			descriptorChanged = !proto.Equal(&previousTableDesc, n.tableDesc)
			if descriptorChanged {
				fastPath, err := RepartitioningFastPathAvailable(&previousTableDesc, n.tableDesc)
				if err != nil {
					return err
				}
				if !fastPath {
					return fmt.Errorf("data validation is required for this repartitioning, but is currently unimplemented")
				}
				// TODO(dan): Remove zone configs which no longer point at a
				// partition.
			}
		default:
			return fmt.Errorf("unsupported alter command: %T", cmd)
		}
	}

	if err := n.tableDesc.AllocateIDs(); err != nil {
		return err
	}

	addedMutations := len(n.tableDesc.Mutations) > origNumMutations
	mutationID := sqlbase.InvalidMutationID
	var err error
	if addedMutations {
		mutationID, err = params.p.createSchemaChangeJob(params.ctx, n.tableDesc,
			tree.AsStringWithFlags(n.n, tree.FmtSimpleQualified))
	} else {
		err = n.tableDesc.SetUpVersion()
	}
	if err != nil {
		return err
	}

	if err := params.p.writeTableDesc(params.ctx, n.tableDesc); err != nil {
		return err
	}

	// Record this index alteration in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	if err := MakeEventLogger(params.p.LeaseMgr()).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogAlterIndex,
		int32(n.tableDesc.ID),
		int32(params.evalCtx.NodeID),
		struct {
			TableName  string
			IndexName  string
			Statement  string
			User       string
			MutationID uint32
		}{n.tableDesc.Name, n.indexDesc.Name, n.n.String(), params.p.session.User, uint32(mutationID)},
	); err != nil {
		return err
	}

	params.p.notifySchemaChange(n.tableDesc, mutationID)

	return nil
}

func (n *alterIndexNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterIndexNode) Close(context.Context)        {}
