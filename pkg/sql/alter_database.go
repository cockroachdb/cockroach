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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

type alterDatabaseOwnerNode struct {
	n    *tree.AlterDatabaseOwner
	desc *dbdesc.Mutable
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

	if err := p.checkCanAlterToNewOwner(ctx, dbDesc, n.Owner); err != nil {
		return nil, err
	}

	// To alter the owner, the user also has to have CREATEDB privilege.
	if err := p.CheckRoleOption(ctx, roleoption.CREATEDB); err != nil {
		return nil, err
	}

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

// AlterDatabaseAddRegion transforms a tree.AlterDatabaseAddRegion into a plan node.
func (p *planner) AlterDatabaseAddRegion(
	ctx context.Context, n *tree.AlterDatabaseAddRegion,
) (planNode, error) {
	return nil, unimplemented.New("alter database add region", "implementation pending")
}

// AlterDatabaseDropRegion transforms a tree.AlterDatabaseDropRegion into a plan node.
func (p *planner) AlterDatabaseDropRegion(
	ctx context.Context, n *tree.AlterDatabaseDropRegion,
) (planNode, error) {
	return nil, unimplemented.New("alter database drop region", "implementation pending")
}

// AlterDatabaseSurvive transforms a tree.AlterDatabaseSurvive into a plan node.
func (p *planner) AlterDatabaseSurvive(
	ctx context.Context, n *tree.AlterDatabaseSurvive,
) (planNode, error) {
	return nil, unimplemented.New("alter database survive", "implementation pending")
}
