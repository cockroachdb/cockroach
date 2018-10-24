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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

var errEmptyIndexName = pgerror.NewError(pgerror.CodeSyntaxError, "empty index name")

type renameIndexNode struct {
	n         *tree.RenameIndex
	tableDesc *sqlbase.MutableTableDescriptor
	idx       sqlbase.IndexDescriptor
}

// RenameIndex renames the index.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) RenameIndex(ctx context.Context, n *tree.RenameIndex) (planNode, error) {
	_, tableDesc, err := expandMutableIndexName(ctx, p, n.Index, true /* requireTable */)
	if err != nil {
		return nil, err
	}

	idx, _, err := tableDesc.FindIndexByName(string(n.Index.Index))
	if err != nil {
		if n.IfExists {
			// Noop.
			return newZeroNode(nil /* columns */), nil
		}
		// Index does not exist, but we want it to: error out.
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &renameIndexNode{n: n, idx: idx, tableDesc: tableDesc}, nil
}

func (n *renameIndexNode) startExec(params runParams) error {
	p := params.p
	ctx := params.ctx
	tableDesc := n.tableDesc
	idx := n.idx

	for _, tableRef := range tableDesc.DependedOnBy {
		if tableRef.IndexID != idx.ID {
			continue
		}
		return p.dependentViewRenameError(
			ctx, "index", n.n.Index.Index.String(), tableDesc.ParentID, tableRef.ID)
	}

	if n.n.NewName == "" {
		return errEmptyIndexName
	}

	if n.n.Index.Index == n.n.NewName {
		// Noop.
		return nil
	}

	if _, _, err := tableDesc.FindIndexByName(string(n.n.NewName)); err == nil {
		return fmt.Errorf("index name %q already exists", string(n.n.NewName))
	}

	if err := tableDesc.RenameIndexDescriptor(idx, string(n.n.NewName)); err != nil {
		return err
	}

	if err := tableDesc.Validate(ctx, p.txn, p.EvalContext().Settings); err != nil {
		return err
	}

	return p.writeSchemaChange(ctx, tableDesc, sqlbase.InvalidMutationID)
}

func (n *renameIndexNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameIndexNode) Close(context.Context)        {}
