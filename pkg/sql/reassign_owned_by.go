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
	// Check all roles in old roles exist
	for _, oldRole := range n.OldRoles {
		roleExists, err := p.RoleExists(ctx, oldRole)
		if err != nil {
			return nil, err
		}

		if !roleExists {
			return nil, pgerror.Newf(pgcode.UndefinedObject, "role/user %q does not exist", oldRole)
		}
	}

	return &reassignOwnedByNode{n: n}, nil
}

func (n *reassignOwnedByNode) startExec(params runParams) error {
	// Search within all descriptors - not limited to this database
	allDescs, err := params.p.Descriptors().GetAllDescriptors(params.ctx, params.p.txn)
	if err != nil {
		return err
	}

	lCtx := newInternalLookupCtx(params.ctx, allDescs, nil)

	// For each role in old roles
	for _, oldRole := range n.n.OldRoles {
		// Iterate through each object in current DB
		for _, dbDesc := range lCtx.dbDescs {
			if params.p.IsOwner(dbDesc, oldRole) {
				mutableDbDesc, err := params.p.Descriptors().GetMutableDescriptorByID(params.ctx, dbDesc.ID, params.p.txn)
				if err != nil {
					return err
				}
				if err := params.p.checkCanAlterDatabaseAndSetNewOwner(
					params.ctx, mutableDbDesc.(*dbdesc.Mutable), n.n.NewRole, true /* hasOwnership */); err != nil {
						return err
				}

				// TODO(angelaw): I noticed that writing the changes requires the relevant node,
				// so moved the logic here instead of within helper method - does this look right?
				// Otherwise I would need to write a helper method that took in any node and pass the node in
				if err := params.p.writeNonDropDatabaseChange(
					params.ctx,
					mutableDbDesc.(*dbdesc.Mutable),
					tree.AsStringWithFQNames(n.n, params.p.Ann()),
				); err != nil {
					return err
				}
			}
		}
		for _, schemaDesc := range lCtx.schemaDescs {
			if params.p.IsOwner(schemaDesc, oldRole) {
				mutableSchemaDesc, err := params.p.Descriptors().GetMutableDescriptorByID(params.ctx, schemaDesc.ID, params.p.txn)
				if err != nil {
					return err
				}
				if err := params.p.checkCanAlterSchemaAndSetNewOwner(
					params.ctx, mutableSchemaDesc.(*schemadesc.Mutable), n.n.NewRole, true /* hasOwnership */);
					err != nil {
						return err
				}
				if err := params.p.writeSchemaDescChange(params.ctx,
					mutableSchemaDesc.(*schemadesc.Mutable),
					tree.AsStringWithFQNames(n.n, params.p.Ann()),
					); err != nil {
						return err
				}
			}
		}

		for _, tbDesc := range lCtx.tbDescs {
			if params.p.IsOwner(tbDesc, oldRole) {
				mutableTbDesc, err := params.p.Descriptors().GetMutableDescriptorByID(params.ctx, tbDesc.ID, params.p.txn)
				if err != nil {
					return err
				}
				if err := params.p.checkCanAlterTableAndSetNewOwner(
					params.ctx, mutableTbDesc.(*tabledesc.Mutable), n.n.NewRole, true /* hasOwnership */); err != nil {
						return err
				}
				// TODO (angelaw): Seems like alter table owner does not have a write change - is this right?
			}
		}

		for _, typDesc := range lCtx.typDescs {
			if params.p.IsOwner(typDesc, oldRole) {
				mutableTypDesc, err := params.p.Descriptors().GetMutableDescriptorByID(params.ctx, typDesc.ID, params.p.txn)
				if err != nil {
					return err
				}
				if err := params.p.checkCanAlterTypeAndSetNewOwner(
					params.ctx, mutableTypDesc.(*typedesc.Mutable), n.n.NewRole, true /* hasOwnership */); err != nil {
						return err
				}
				if err := params.p.writeTypeSchemaChange(
					params.ctx, mutableTypDesc.(*typedesc.Mutable), tree.AsStringWithFQNames(n.n, params.p.Ann()),
				); err != nil {
					return err
				}
				arrayDesc, err := params.p.Descriptors().GetMutableTypeVersionByID(params.ctx, params.p.txn, typDesc.ArrayTypeID)
				if err != nil {
					return err
				}
				if err := params.p.writeTypeSchemaChange(
					params.ctx, arrayDesc, tree.AsStringWithFQNames(n.n, params.p.Ann()),
				); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (n *reassignOwnedByNode) Next(runParams) (bool, error) { return false, nil }
func (n *reassignOwnedByNode) Values() tree.Datums          { return tree.Datums{} }
func (n *reassignOwnedByNode) Close(context.Context)        {}
