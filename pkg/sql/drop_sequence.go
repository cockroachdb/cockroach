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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type dropSequenceNode struct {
	n  *tree.DropSequence
	td []toDelete
}

func (p *planner) DropSequence(ctx context.Context, n *tree.DropSequence) (planNode, error) {
	td := make([]toDelete, 0, len(n.Names))
	for i := range n.Names {
		tn, err := n.Names[i].Normalize()
		if err != nil {
			return nil, err
		}
		droppedDesc, err := p.prepareDrop(ctx, tn, !n.IfExists, requireSequenceDesc)
		if err != nil {
			return nil, err
		}
		if droppedDesc == nil {
			// IfExists specified and descriptor does not exist.
			continue
		}

		if depErr := p.sequenceDependencyError(ctx, droppedDesc); depErr != nil {
			return nil, depErr
		}

		td = append(td, toDelete{tn, droppedDesc})
	}

	if len(td) == 0 {
		return newZeroNode(nil /* columns */), nil
	}

	return &dropSequenceNode{
		n:  n,
		td: td,
	}, nil
}

func (n *dropSequenceNode) startExec(params runParams) error {
	ctx := params.ctx
	for _, toDel := range n.td {
		droppedDesc := toDel.desc
		err := params.p.dropSequenceImpl(ctx, droppedDesc, n.n.DropBehavior)
		if err != nil {
			return err
		}
		// Log a Drop Sequence event for this table. This is an auditable log event
		// and is recorded in the same transaction as the table descriptor
		// update.
		if err := MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
			ctx,
			params.p.txn,
			EventLogDropSequence,
			int32(droppedDesc.ID),
			int32(params.extendedEvalCtx.NodeID),
			struct {
				SequenceName string
				Statement    string
				User         string
			}{toDel.tn.FQString(), n.n.String(), params.SessionData().User},
		); err != nil {
			return err
		}
	}
	return nil
}

func (*dropSequenceNode) Next(runParams) (bool, error) { return false, nil }
func (*dropSequenceNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropSequenceNode) Close(context.Context)        {}

func (p *planner) dropSequenceImpl(
	ctx context.Context, seqDesc *sqlbase.TableDescriptor, behavior tree.DropBehavior,
) error {
	return p.initiateDropTable(ctx, seqDesc, true /* drainName */)
}

// sequenceDependency error returns an error if the given sequence cannot be dropped because
// a table uses it in a DEFAULT expression on one of its columns, or nil if there is no
// such dependency.
func (p *planner) sequenceDependencyError(
	ctx context.Context, droppedDesc *sqlbase.TableDescriptor,
) error {
	if len(droppedDesc.DependedOnBy) > 0 {
		return pgerror.NewErrorf(
			pgerror.CodeDependentObjectsStillExistError,
			"cannot drop sequence %s because other objects depend on it",
			droppedDesc.Name,
		)
	}
	return nil
}
