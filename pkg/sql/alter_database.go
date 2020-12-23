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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

type alterDatabaseOwnerNode struct {
	n    *tree.AlterDatabaseOwner
	desc *dbdesc.Mutable
}

// AlterDatabaseOwner transforms a tree.AlterDatabaseOwner into a plan node.
func (p *planner) AlterDatabaseOwner(
	ctx context.Context, n *tree.AlterDatabaseOwner,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DATABASE",
	); err != nil {
		return nil, err
	}

	_, dbDesc, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, n.Name.String(),
		tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return nil, err
	}

	return &alterDatabaseOwnerNode{n: n, desc: dbDesc}, nil
}

func (n *alterDatabaseOwnerNode) startExec(params runParams) error {
	privs := n.desc.GetPrivileges()

	// If the owner we want to set to is the current owner, do a no-op.
	newOwner := n.n.Owner
	if newOwner == privs.Owner() {
		return nil
	}
	if err := params.p.checkCanAlterDatabaseAndSetNewOwner(params.ctx, n.desc, newOwner); err != nil {
		return err
	}

	if err := params.p.writeNonDropDatabaseChange(
		params.ctx,
		n.desc,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	return nil
}

// checkCanAlterDatabaseAndSetNewOwner handles privilege checking and setting new owner.
// Called in ALTER DATABASE and REASSIGN OWNED BY.
func (p *planner) checkCanAlterDatabaseAndSetNewOwner(
	ctx context.Context, desc catalog.MutableDescriptor, newOwner security.SQLUsername,
) error {
	if err := p.checkCanAlterToNewOwner(ctx, desc, newOwner); err != nil {
		return err
	}

	// To alter the owner, the user also has to have CREATEDB privilege.
	if err := p.CheckRoleOption(ctx, roleoption.CREATEDB); err != nil {
		return err
	}

	privs := desc.GetPrivileges()
	privs.SetOwner(newOwner)

	// Log Alter Database Owner event. This is an auditable log event and is recorded
	// in the same transaction as the table descriptor update.
	return p.logEvent(ctx,
		desc.GetID(),
		&eventpb.AlterDatabaseOwner{
			DatabaseName: desc.GetName(),
			Owner:        newOwner.Normalized(),
		})
}

func (n *alterDatabaseOwnerNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterDatabaseOwnerNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterDatabaseOwnerNode) Close(context.Context)        {}

// AlterDatabaseAddRegion transforms a tree.AlterDatabaseAddRegion into a plan node.
func (p *planner) AlterDatabaseAddRegion(
	ctx context.Context, n *tree.AlterDatabaseAddRegion,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DATABASE",
	); err != nil {
		return nil, err
	}
	return nil, unimplemented.New("alter database add region", "implementation pending")
}

// AlterDatabaseDropRegion transforms a tree.AlterDatabaseDropRegion into a plan node.
func (p *planner) AlterDatabaseDropRegion(
	ctx context.Context, n *tree.AlterDatabaseDropRegion,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DATABASE",
	); err != nil {
		return nil, err
	}
	return nil, unimplemented.New("alter database drop region", "implementation pending")
}

// AlterDatabasePrimaryRegion transforms a tree.AlterDatabasePrimaryRegion into a plan node.
func (p *planner) AlterDatabasePrimaryRegion(
	ctx context.Context, n *tree.AlterDatabasePrimaryRegion,
) (planNode, error) {
	return nil, unimplemented.New("alter database primary region", "implementation pending")
}

// AlterDatabaseSurvivalGoal transforms a tree.AlterDatabaseSurvivalGoal into a plan node.
func (p *planner) AlterDatabaseSurvivalGoal(
	ctx context.Context, n *tree.AlterDatabaseSurvivalGoal,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER DATABASE",
	); err != nil {
		return nil, err
	}
	return nil, unimplemented.New("alter database survive", "implementation pending")
}
