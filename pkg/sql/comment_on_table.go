// Copyright 2018 The Cockroach Authors.
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
)

type commentOnTableNode struct {
	n         *tree.CommentOnTable
	tableDesc *MutableTableDescriptor
}

// CommentOnTable add comment on a table.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) CommentOnTable(ctx context.Context, n *tree.CommentOnTable) (planNode, error) {
	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &n.Table, true, requireTableDesc)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return newZeroNode(nil /* columns */), nil
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &commentOnTableNode{n: n, tableDesc: tableDesc}, nil
}

func (n *commentOnTableNode) startExec(params runParams) error {
	n.tableDesc.Comment = n.n.Comment

	mutationID, err := params.p.createSchemaChangeJob(
		params.ctx,
		n.tableDesc,
		tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames))
	if err != nil {
		return err
	}

	if err := params.p.writeSchemaChange(params.ctx, n.tableDesc, mutationID); err != nil {
		return err
	}

	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogCommentOnTable,
		int32(n.tableDesc.ID),
		int32(params.extendedEvalCtx.NodeID),
		struct {
			TableName  string
			Statement  string
			User       string
			MutationID uint32
			Comment    *string
		}{
			n.n.Table.FQString(),
			n.n.String(),
			params.SessionData().User,
			uint32(mutationID),
			n.n.Comment},
	)
}

func (n *commentOnTableNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnTableNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnTableNode) Close(context.Context)        {}
