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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ReassignOwnedByNode represents a REASSIGN OWNED BY <name> TO <name> statement.
type reassignOwnedByNode struct {
	n *tree.ReassignOwnedBy
}

func (p *planner) ReassignOwnedBy(ctx context.Context, n *tree.ReassignOwnedBy) (planNode, error) {
	/* TODO(angelaw): only leave checks for old roles existing after authorization.go file is updated */
	// Check all roles in old roles exist and that current user is a member of all.
	currentUser := p.SessionData().User

	memberOf, err := p.MemberOfWithAdminOption(ctx, currentUser)
	if err != nil {
		return nil, err
	}

	for _, oldRole := range n.OldRoles {
		roleExists, err := p.RoleExists(ctx, oldRole)
		if err != nil {
			return nil, err
		}
		if !roleExists {
			return nil, pgerror.Newf(pgcode.UndefinedObject, "role/user %q does not exist", oldRole)
		}

		if _, ok := memberOf[oldRole]; !ok {
			return nil, pgerror.Newf(
				pgcode.InsufficientPrivilege, "must be member of role %q", oldRole)
		}
	}

	// Check new role exists and that user is member of new role, even if old roles do not own anything.
	roleExists, err := p.RoleExists(ctx, n.NewRole)
	if err != nil {
		return nil, err
	}
	if !roleExists {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "role/user %q does not exist", n.NewRole)
	}
	if _, ok := memberOf[n.NewRole]; !ok {
		return nil, pgerror.Newf(
			pgcode.InsufficientPrivilege, "must be member of role %q", n.NewRole)
	}

	return &reassignOwnedByNode{n: n}, nil
}

func (n *reassignOwnedByNode) startExec(params runParams) error {
	// Search within all descriptors in all databases. Postgres limits this command to
	// reassigning only within the current database, but we are implementing the
	// functionality to all databases.
	allDescs, err := params.p.Descriptors().GetAllDescriptors(params.ctx, params.p.txn)
	if err != nil {
		return err
	}

	lCtx := newInternalLookupCtx(params.ctx, allDescs, nil)

	// Iterate through each object, check for ownership by an old role.
	for _, oldRole := range n.n.OldRoles {
		for _, dbDesc := range lCtx.dbDescs {
			if params.p.IsOwner(dbDesc, oldRole) {
				if err := n.reassignDatabaseOwner(dbDesc, oldRole, params); err != nil {
					return err
				}
			}
		}
		for _, schemaDesc := range lCtx.schemaDescs {
			if params.p.IsOwner(schemaDesc, oldRole) {
				if err := n.reassignSchemaOwner(schemaDesc, oldRole, params); err != nil {
					return err
				}
			}
		}
		for _, tbDesc := range lCtx.tbDescs {
			if params.p.IsOwner(tbDesc, oldRole) {
				if err := n.reassignTableOwner(tbDesc, oldRole, params); err != nil {
					return err
				}
			}
		}

		for _, typDesc := range lCtx.typDescs {
			if params.p.IsOwner(typDesc, oldRole) && (typDesc.Kind != descpb.TypeDescriptor_ALIAS) {
				if err := n.reassignTypeOwner(typDesc, oldRole, params); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (n *reassignOwnedByNode) reassignDatabaseOwner(
	dbDesc *dbdesc.Immutable, oldRole string, params runParams,
) error {
	mutableDbDesc, err := params.p.Descriptors().GetMutableDescriptorByID(params.ctx, dbDesc.ID, params.p.txn)
	if err != nil {
		return err
	}
	if err := params.p.checkCanAlterDatabaseAndSetNewOwner(
		params.ctx, mutableDbDesc.(*dbdesc.Mutable), n.n.NewRole, true /* hasOwnership */); err != nil {
		return err
	}
	/* TODO(angelaw): if I can directly call the original methods like alterDatabaseOwner
	then I don't need to also write the changes separately here. Pending Solon's changes
	to authorization.go */
	if err := params.p.writeNonDropDatabaseChange(
		params.ctx,
		mutableDbDesc.(*dbdesc.Mutable),
		tree.AsStringWithFQNames(n.n, params.p.Ann()),
	); err != nil {
		return err
	}
	return nil
}

func (n *reassignOwnedByNode) reassignSchemaOwner(
	schemaDesc *schemadesc.Immutable, oldRole string, params runParams,
) error {
	mutableSchemaDesc, err := params.p.Descriptors().GetMutableDescriptorByID(
		params.ctx, schemaDesc.ID, params.p.txn)
	if err != nil {
		return err
	}
	if err := params.p.checkCanAlterSchemaAndSetNewOwner(
		params.ctx, mutableSchemaDesc.(*schemadesc.Mutable), n.n.NewRole, true /* hasOwnership */); err != nil {
		return err
	}
	/* TODO(angelaw): if I can directly call the original methods like alterDatabaseOwner
	then I don't need to also write the changes separately here. Pending Solon's changes
	to authorization.go */
	if err := params.p.writeSchemaDescChange(params.ctx,
		mutableSchemaDesc.(*schemadesc.Mutable),
		tree.AsStringWithFQNames(n.n, params.p.Ann()),
	); err != nil {
		return err
	}
	return nil
}

func (n *reassignOwnedByNode) reassignTableOwner(
	tbDesc *tabledesc.Immutable, oldRole string, params runParams,
) error {
	mutableTbDesc, err := params.p.Descriptors().GetMutableDescriptorByID(
		params.ctx, tbDesc.ID, params.p.txn)
	if err != nil {
		return err
	}
	if err := params.p.checkCanAlterTableAndSetNewOwner(
		params.ctx, mutableTbDesc.(*tabledesc.Mutable), n.n.NewRole, true /* hasOwnership */); err != nil {
		return err
	}
	/* TODO(angelaw): if I can directly call the original methods like alterDatabaseOwner
	then I don't need to also write the changes separately here. Pending Solon's changes
	to authorization.go */
	if err := params.p.writeSchemaChange(
		params.ctx, mutableTbDesc.(*tabledesc.Mutable), descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}
	return nil
}

func (n *reassignOwnedByNode) reassignTypeOwner(
	typDesc *typedesc.Immutable, oldRole string, params runParams,
) error {
	mutableTypDesc, err := params.p.Descriptors().GetMutableDescriptorByID(
		params.ctx, typDesc.ID, params.p.txn)
	if err != nil {
		return err
	}
	arrayDesc, err := params.p.Descriptors().GetMutableTypeVersionByID(
		params.ctx, params.p.txn, typDesc.ArrayTypeID)
	if err != nil {
		return err
	}
	if err := params.p.checkCanAlterTypeAndSetNewOwner(
		params.ctx, mutableTypDesc.(*typedesc.Mutable), arrayDesc, n.n.NewRole, true /* hasOwnership */); err != nil {
		return err
	}
	/* TODO(angelaw): if I can directly call the original methods like alterDatabaseOwner
	then I don't need to also write the changes separately here. Pending Solon's changes
	to authorization.go */
	if err := params.p.writeTypeSchemaChange(
		params.ctx, mutableTypDesc.(*typedesc.Mutable), tree.AsStringWithFQNames(n.n, params.p.Ann()),
	); err != nil {
		return err
	}
	if err := params.p.writeTypeSchemaChange(
		params.ctx, arrayDesc, tree.AsStringWithFQNames(n.n, params.p.Ann()),
	); err != nil {
		return err
	}
	return nil
}

func (n *reassignOwnedByNode) Next(runParams) (bool, error) { return false, nil }
func (n *reassignOwnedByNode) Values() tree.Datums          { return tree.Datums{} }
func (n *reassignOwnedByNode) Close(context.Context)        {}
