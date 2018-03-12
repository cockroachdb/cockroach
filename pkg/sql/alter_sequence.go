// Copyright 2015 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type alterSequenceNode struct {
	n       *tree.AlterSequence
	seqDesc *sqlbase.TableDescriptor
}

// AlterSequence transforms a tree.AlterSequence into a plan node.
func (p *planner) AlterSequence(ctx context.Context, n *tree.AlterSequence) (planNode, error) {
	tn, err := n.Name.Normalize()
	if err != nil {
		return nil, err
	}

	var seqDesc *TableDescriptor
	// DDL statements avoid the cache to avoid leases, and can view non-public descriptors.
	// TODO(vivek): check if the cache can be used.
	p.runWithOptions(resolveFlags{allowAdding: true, skipCache: true}, func() {
		seqDesc, err = ResolveExistingObject(ctx, p, tn, !n.IfExists, requireSequenceDesc)
	})
	if err != nil {
		return nil, err
	}
	if seqDesc == nil {
		return newZeroNode(nil /* columns */), nil
	}

	if err := p.CheckPrivilege(ctx, seqDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &alterSequenceNode{n: n, seqDesc: seqDesc}, nil
}

func (n *alterSequenceNode) startExec(params runParams) error {
	desc := n.seqDesc

	err := assignSequenceOptions(desc.SequenceOpts, n.n.Options, false /* setDefaults */)
	if err != nil {
		return err
	}

	if err := params.p.writeTableDesc(params.ctx, n.seqDesc); err != nil {
		return err
	}

	// Record this sequence alteration in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	if err := MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogAlterSequence,
		int32(n.seqDesc.ID),
		int32(params.extendedEvalCtx.NodeID),
		struct {
			SequenceName string
			Statement    string
			User         string
		}{n.n.Name.TableName().FQString(), n.n.String(), params.SessionData().User},
	); err != nil {
		return err
	}

	params.p.notifySchemaChange(n.seqDesc, sqlbase.InvalidMutationID)

	return nil
}

func (n *alterSequenceNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterSequenceNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterSequenceNode) Close(context.Context)        {}
