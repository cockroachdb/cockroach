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

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type dropSequenceNode struct {
	n  *tree.DropSequence
	td []*sqlbase.TableDescriptor
}

func (p *planner) DropSequence(ctx context.Context, n *tree.DropSequence) (planNode, error) {
	td := make([]*sqlbase.TableDescriptor, 0, len(n.Names))
	for _, name := range n.Names {
		tn, err := name.NormalizeTableName()
		if err != nil {
			return nil, err
		}
		if err := tn.QualifyWithDatabase(p.SessionData().Database); err != nil {
			return nil, err
		}

		droppedDesc, err := p.dropTableOrViewPrepare(ctx, tn)
		if err != nil {
			return nil, err
		}
		if droppedDesc == nil {
			if n.IfExists {
				continue
			}
			// Sequence does not exist, but we want it to: error out.
			return nil, sqlbase.NewUndefinedRelationError(tn)
		}
		if !droppedDesc.IsSequence() {
			return nil, sqlbase.NewWrongObjectTypeError(tn, "sequence")
		}

		td = append(td, droppedDesc)
	}

	if len(td) == 0 {
		return &zeroNode{}, nil
	}

	return &dropSequenceNode{
		n:  n,
		td: td,
	}, nil
}

func (n *dropSequenceNode) startExec(params runParams) error {
	ctx := params.ctx
	for _, droppedDesc := range n.td {
		if droppedDesc == nil {
			continue
		}
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
			}{droppedDesc.Name, n.n.String(), params.SessionData().User},
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
	err := p.initiateDropTable(ctx, seqDesc)
	if err != nil {
		return err
	}

	p.testingVerifyMetadata().setTestingVerifyMetadata(
		func(systemConfig config.SystemConfig) error {
			return verifyDropTableMetadata(systemConfig, seqDesc.ID, "sequence")
		})

	return nil
}
