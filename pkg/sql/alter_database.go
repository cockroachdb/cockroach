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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type alterDatabaseOwnerNode struct {
	n    *tree.AlterDatabaseOwner
	desc *dbdesc.Mutable
}

// AlterDatabaseOwner transforms a tree.AlterDatabaseOwner into a plan node.
func (p *planner) AlterDatabaseOwner(
	ctx context.Context, n *tree.AlterDatabaseOwner,
) (planNode, error) {
	dbDesc, err := p.ResolveMutableDatabaseDescriptor(ctx, n.Name.String(), true /* required */)
	if err != nil {
		return nil, err
	}

	return &alterDatabaseOwnerNode{n: n, desc: dbDesc}, nil
}

func (n *alterDatabaseOwnerNode) startExec(params runParams) error {
	if err := params.p.checkCanAlterDatabaseAndSetNewOwner(params.ctx, n.desc, n.n.Owner); err != nil {
		return err
	}

	return params.p.writeNonDropDatabaseChange(
		params.ctx,
		n.desc,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	)
}

// checkCanAlterDatabaseAndSetNewOwner handles privilege checking and setting new owner.
// Called in ALTER DATABASE and REASSIGN OWNED BY.
func (p *planner) checkCanAlterDatabaseAndSetNewOwner(
	ctx context.Context, desc catalog.MutableDescriptor, newOwner string,
) error {
	privs := desc.GetPrivileges()

	// If the owner we want to set to is the current owner, do a no-op.
	if newOwner == privs.Owner {
		return nil
	}

	if err := p.checkCanAlterToNewOwner(ctx, desc, newOwner); err != nil {
		return err
	}

	// To alter the owner, the user also has to have CREATEDB privilege.
	if err := p.CheckRoleOption(ctx, roleoption.CREATEDB); err != nil {
		return err
	}

	privs.SetOwner(newOwner)

	return nil
}

func (n *alterDatabaseOwnerNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterDatabaseOwnerNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterDatabaseOwnerNode) Close(context.Context)        {}
