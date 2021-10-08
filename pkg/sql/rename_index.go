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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var errEmptyIndexName = pgerror.New(pgcode.Syntax, "empty index name")

type renameIndexNode struct {
	n         *tree.RenameIndex
	tableDesc *tabledesc.Mutable
	idx       catalog.Index
}

// RenameIndex renames the index.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) RenameIndex(ctx context.Context, n *tree.RenameIndex) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"RENAME INDEX",
	); err != nil {
		return nil, err
	}

	_, tableDesc, err := expandMutableIndexName(ctx, p, n.Index, !n.IfExists /* requireTable */)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		// IfExists specified and table did not exist -- noop.
		return newZeroNode(nil /* columns */), nil
	}

	idx, err := tableDesc.FindIndexWithName(string(n.Index.Index))
	if err != nil {
		if n.IfExists {
			// Noop.
			return newZeroNode(nil /* columns */), nil
		}
		// Index does not exist, but we want it to: error out.
		return nil, pgerror.WithCandidateCode(err, pgcode.UndefinedObject)
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
		if tableRef.IndexID != idx.GetID() {
			continue
		}
		return p.dependentViewError(
			ctx, "index", n.n.Index.Index.String(), tableDesc.ParentID, tableRef.ID, "rename",
		)
	}

	if n.n.NewName == "" {
		return errEmptyIndexName
	}

	if n.n.Index.Index == n.n.NewName {
		// Noop.
		return nil
	}

	if foundIndex, _ := tableDesc.FindIndexWithName(string(n.n.NewName)); foundIndex != nil {
		return pgerror.Newf(pgcode.DuplicateRelation, "index name %q already exists", string(n.n.NewName))
	}

	idx.IndexDesc().Name = string(n.n.NewName)

	if err := validateDescriptor(ctx, p, tableDesc); err != nil {
		return err
	}

	return p.writeSchemaChange(
		ctx, tableDesc, descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()))
}

func (n *renameIndexNode) Next(runParams) (bool, error) { return false, nil }
func (n *renameIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (n *renameIndexNode) Close(context.Context)        {}
