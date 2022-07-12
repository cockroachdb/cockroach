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

type alterIndexVisibleNode struct {
	n         *tree.AlterIndexVisible
	tableDesc *tabledesc.Mutable
	idx       catalog.Index
}

// ALTER INDEX ... [NOT VISIBLE | VISIBLE]  alters the visibility of the index.
// TODO (wenyihu6): check comments here
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) AlterIndexVisible(ctx context.Context, n *tree.AlterIndexVisible) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER INDEX VISIBLILITY",
	); err != nil {
		return nil, err
	}

	// Find the underlying table.
	_, tableDesc, err := expandMutableIndexName(ctx, p, n.Index, !n.IfExists /* requireTable */)

	if err != nil {
		// Error if no table exists and IfExists is not specified.
		return nil, err
	}

	if tableDesc == nil {
		// Nothing needed if no table exists and IfExists is specified.
		return newZeroNode(nil /* columns */), nil
	}

	// Now we know this table exists, find the index in the table.
	idx, err := tableDesc.FindIndexWithName(string(n.Index.Index))
	if err != nil {
		if n.IfExists {
			// Nothing needed if no index exists and IfExists is specified.
			return newZeroNode(nil /* columns */), nil
		}
		// Error if no index exists and IfExists is not specified.
		return nil, pgerror.WithCandidateCode(err, pgcode.UndefinedObject)
	}

	// TODO(wenyihu6): check which privilege should be checked here for alter index invisible
	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	if idx.IsHidden() == n.Hidden {
		// Nothing needed if the visibility of the index is already what they want.
		// TODO (wenyihu6): give hint that it is already not visible or already visible
		return newZeroNode(nil /* columns */), nil
	}

	return &alterIndexVisibleNode{n: n, tableDesc: tableDesc, idx: idx}, nil
}

func (n *alterIndexVisibleNode) startExec(params runParams) error {
	p := params.p
	ctx := params.ctx
	tableDesc := n.tableDesc
	idx := n.idx

	// TODO(wenyihu6): do we need to check if any view depends on it? the select query saved in view will have different
	// output after index is changed to invisible -> add test case for this

	// TODO (wenyihu6): add more check for edge cases here
	if n.n.Hidden {
		if n.idx.Primary() {
			return pgerror.New(pgcode.FeatureNotSupported, "a primary index cannot be invisible.")
		} else if n.idx.IsUnique() {
			return pgerror.New(pgcode.FeatureNotSupported, "invisible unique index is currently not supported.")
		}
	}

	idx.IndexDesc().Hidden = n.n.Hidden

	if err := validateDescriptor(ctx, p, tableDesc); err != nil {
		return err
	}

	return p.writeSchemaChange(
		ctx, tableDesc, descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()))
}

func (n *alterIndexVisibleNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterIndexVisibleNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterIndexVisibleNode) Close(context.Context)        {}
