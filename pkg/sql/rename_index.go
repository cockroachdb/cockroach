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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

var errEmptyIndexName = pgerror.New(pgcode.Syntax, "empty index name")

type renameIndexNode struct {
	n         *tree.RenameIndex
	tableDesc *sqlbase.MutableTableDescriptor
	idx       *sqlbase.IndexDescriptor
}

// RenameIndex renames the index.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) RenameIndex(ctx context.Context, n *tree.RenameIndex) (planNode, error) {
	_, tableDesc, err := expandMutableIndexName(ctx, p, n.Index, !n.IfExists /* requireTable */)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		// IfExists specified and table did not exist -- noop.
		return newZeroNode(nil /* columns */), nil
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

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because RENAME DATABASE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *renameIndexNode) ReadingOwnWrites() {}

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

	if err := tableDesc.Validate(ctx, p.txn, p.ExecCfg().Codec); err != nil {
		return err
	}

	return p.writeSchemaChange(
		ctx, tableDesc, sqlbase.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()))
}

func (n *renameIndexNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameIndexNode) Close(context.Context)        {}
