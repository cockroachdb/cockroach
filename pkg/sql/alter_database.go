// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type alterDatabaseOwnerNode struct {
	n    *tree.AlterDatabaseOwner
	desc *sqlbase.MutableDatabaseDescriptor
}

// AlterDatabaseOwner transforms a tree.AlterDatabaseOwner into a plan node.
func (p *planner) AlterDatabaseOwner(
	ctx context.Context, n *tree.AlterDatabaseOwner,
) (planNode, error) {
	dbDesc, err := p.ResolveMutableDatabaseDescriptor(ctx, n.Name.String(), true)
	if err != nil {
		return nil, err
	}
	privs := dbDesc.GetPrivileges()

	hasOwnership, err := p.HasOwnership(ctx, dbDesc)
	if err != nil {
		return nil, err
	}

	if err := p.checkCanAlterToNewOwner(ctx, dbDesc, privs, n.Owner, hasOwnership); err != nil {
		return nil, err
	}

	// To alter the owner, the user also has to have CREATEDB privilege.
	// TODO(richardjcai): Add this check once #52576 is implemented.

	// If the owner we want to set to is the current owner, do a no-op.
	if n.Owner == privs.Owner {
		return nil, nil
	}
	return &alterDatabaseOwnerNode{n: n, desc: dbDesc}, nil
}

func (n *alterDatabaseOwnerNode) startExec(params runParams) error {
	n.desc.GetPrivileges().SetOwner(n.n.Owner)
	return params.p.writeNonDropDatabaseChange(
		params.ctx,
		n.desc,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	)
}

func (n *alterDatabaseOwnerNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterDatabaseOwnerNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterDatabaseOwnerNode) Close(context.Context)        {}
