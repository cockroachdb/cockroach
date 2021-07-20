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

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

// ReassignOwnedByNode represents a REASSIGN OWNED BY <role(s)> TO <role> statement.
type reassignOwnedByNode struct {
	n *tree.ReassignOwnedBy
}

func (p *planner) ReassignOwnedBy(ctx context.Context, n *tree.ReassignOwnedBy) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"REASSIGN OWNED BY",
	); err != nil {
		return nil, err
	}

	// Check all roles in old roles exist. Checks in authorization.go will confirm that current user
	// is a member of old roles and new roles and has CREATE privilege.
	for _, oldRole := range n.OldRoles {
		roleExists, err := RoleExists(ctx, p.ExecCfg(), p.Txn(), oldRole)
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
	telemetry.Inc(sqltelemetry.CreateReassignOwnedByCounter())

	allDescs, err := params.p.Descriptors().GetAllDescriptors(params.ctx, params.p.txn)
	if err != nil {
		return err
	}

	// Filter for all objects in current database.
	currentDatabase := params.p.CurrentDatabase()
	currentDbDesc, err := params.p.Descriptors().GetMutableDatabaseByName(
		params.ctx, params.p.txn, currentDatabase, tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return err
	}

	lCtx := newInternalLookupCtx(params.ctx, allDescs,
		currentDbDesc.ImmutableCopy().(catalog.DatabaseDescriptor), nil /* fallback */)

	// Iterate through each object, check for ownership by an old role.
	for _, oldRole := range n.n.OldRoles {
		// There should only be one database (current).
		for _, dbID := range lCtx.dbIDs {
			if IsOwner(lCtx.dbDescs[dbID], oldRole) {
				if err := n.reassignDatabaseOwner(lCtx.dbDescs[dbID], params); err != nil {
					return err
				}
			}
		}
		for _, schemaID := range lCtx.schemaIDs {
			if IsOwner(lCtx.schemaDescs[schemaID], oldRole) {
				if err := n.reassignSchemaOwner(lCtx.schemaDescs[schemaID], params); err != nil {
					return err
				}
			}
		}
		for _, tbID := range lCtx.tbIDs {
			if IsOwner(lCtx.tbDescs[tbID], oldRole) {
				if err := n.reassignTableOwner(lCtx.tbDescs[tbID], params); err != nil {
					return err
				}
			}
		}
		for _, typID := range lCtx.typIDs {
			if IsOwner(lCtx.typDescs[typID], oldRole) && (lCtx.typDescs[typID].GetKind() != descpb.TypeDescriptor_ALIAS) {
				if err := n.reassignTypeOwner(lCtx.typDescs[typID], params); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (n *reassignOwnedByNode) reassignDatabaseOwner(
	dbDesc catalog.DatabaseDescriptor, params runParams,
) error {
	mutableDbDesc, err := params.p.Descriptors().GetMutableDescriptorByID(params.ctx, dbDesc.GetID(), params.p.txn)
	if err != nil {
		return err
	}
	if err := params.p.checkCanAlterDatabaseAndSetNewOwner(params.ctx,
		mutableDbDesc.(*dbdesc.Mutable), n.n.NewRole); err != nil {
		return err
	}
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
	schemaDesc catalog.SchemaDescriptor, params runParams,
) error {
	mutableSchemaDesc, err := params.p.Descriptors().GetMutableDescriptorByID(
		params.ctx, schemaDesc.GetID(), params.p.txn)
	if err != nil {
		return err
	}
	if err := params.p.checkCanAlterSchemaAndSetNewOwner(
		params.ctx, mutableSchemaDesc.(*schemadesc.Mutable), n.n.NewRole); err != nil {
		return err
	}
	if err := params.p.writeSchemaDescChange(params.ctx,
		mutableSchemaDesc.(*schemadesc.Mutable),
		tree.AsStringWithFQNames(n.n, params.p.Ann()),
	); err != nil {
		return err
	}
	return nil
}

func (n *reassignOwnedByNode) reassignTableOwner(
	tbDesc catalog.TableDescriptor, params runParams,
) error {
	mutableTbDesc, err := params.p.Descriptors().GetMutableDescriptorByID(
		params.ctx, tbDesc.GetID(), params.p.txn)
	if err != nil {
		return err
	}
	if err := params.p.checkCanAlterTableAndSetNewOwner(
		params.ctx, mutableTbDesc.(*tabledesc.Mutable), n.n.NewRole); err != nil {
		return err
	}
	if err := params.p.writeSchemaChange(
		params.ctx, mutableTbDesc.(*tabledesc.Mutable), descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}
	return nil
}

func (n *reassignOwnedByNode) reassignTypeOwner(
	typDesc catalog.TypeDescriptor, params runParams,
) error {
	mutableTypDesc, err := params.p.Descriptors().GetMutableDescriptorByID(
		params.ctx, typDesc.GetID(), params.p.txn)
	if err != nil {
		return err
	}
	arrayDesc, err := params.p.Descriptors().GetMutableTypeVersionByID(
		params.ctx, params.p.txn, typDesc.GetArrayTypeID())
	if err != nil {
		return err
	}
	if err := params.p.checkCanAlterTypeAndSetNewOwner(
		params.ctx, mutableTypDesc.(*typedesc.Mutable), arrayDesc, n.n.NewRole); err != nil {
		return err
	}
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
