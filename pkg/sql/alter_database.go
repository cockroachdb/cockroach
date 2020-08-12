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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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
	immutableDbDesc, err := p.ResolveUncachedDatabaseByName(ctx, n.Name.String(), true /*required*/)
	if err != nil {
		return nil, err
	}

	dbDesc := sqlbase.NewMutableExistingDatabaseDescriptor(*immutableDbDesc.DatabaseDesc())
	privs := dbDesc.GetPrivileges()

	// Make sure the newOwner exists.
	roleExists, err := p.RoleExists(ctx, n.Owner)
	if err != nil {
		return nil, err
	}
	if !roleExists {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "role/user %s does not exist", n.Owner)
	}

	// To alter the owner, the user also has to have CREATEDB privilege.
	// TODO(richardjcai): Add this check once #52576 is implemented.
	hasOwnership, err := p.HasOwnership(ctx, dbDesc)
	if err != nil {
		return nil, err
	}

	if !hasOwnership {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"must be owner of database %s", tree.Name(dbDesc.GetName()))
	}

	// Requirements from PG:
	// To alter the owner, you must also be a direct or indirect member of the
	// new owning role.
	memberOf, err := p.MemberOfWithAdminOption(ctx, privs.Owner)
	if err != nil {
		return nil, err
	}
	if _, ok := memberOf[n.Owner]; !ok {
		return nil, pgerror.Newf(
			pgcode.InsufficientPrivilege, "must be member of role %s", n.Owner)
	}

	// If the owner we want to set to is the current owner, do a no-op.
	if n.Owner == privs.Owner {
		return nil, nil
	}
	return &alterDatabaseOwnerNode{n: n, desc: dbDesc}, nil
}

func (n *alterDatabaseOwnerNode) startExec(params runParams) error {
	n.desc.GetPrivileges().SetOwner(n.n.Owner)

	return params.p.writeDatabaseDesc(params.ctx, n.desc)
}

func (n *alterDatabaseOwnerNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterDatabaseOwnerNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterDatabaseOwnerNode) Close(context.Context)        {}

func (p *planner) writeDatabaseDesc(
	ctx context.Context, dbDesc *sqlbase.MutableDatabaseDescriptor,
) error {
	// Maybe increment the database's version.
	dbDesc.MaybeIncrementVersion()

	// Add the modified descriptor to the descriptor collection.
	if err := p.Descriptors().AddUncommittedDescriptor(dbDesc); err != nil {
		return err
	}

	// Write the database descriptor out to a new batch.
	b := p.txn.NewBatch()
	if err := catalogkv.WriteDescToBatch(
		ctx,
		p.extendedEvalCtx.Tracing.KVTracingEnabled(),
		p.ExecCfg().Settings,
		b,
		p.ExecCfg().Codec,
		dbDesc.ID,
		dbDesc,
	); err != nil {
		return err
	}
	return p.txn.Run(ctx, b)
}
