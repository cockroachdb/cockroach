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

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// ReassignOwnedByNode represents a REASSIGN OWNED BY <role(s)> TO <role> statement.
type reassignOwnedByNode struct {
	n                  *tree.ReassignOwnedBy
	normalizedOldRoles []username.SQLUsername
}

func (p *planner) ReassignOwnedBy(ctx context.Context, n *tree.ReassignOwnedBy) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"REASSIGN OWNED BY",
	); err != nil {
		return nil, err
	}

	normalizedOldRoles, err := decodeusername.FromRoleSpecList(
		p.SessionData(), username.PurposeValidation, n.OldRoles,
	)
	if err != nil {
		return nil, err
	}
	// Check all roles in old roles exist. Checks in authorization.go will confirm that current user
	// is a member of old roles and new roles and has CREATE privilege.
	// Postgres first checks if the role exists before checking privileges.
	for _, oldRole := range normalizedOldRoles {
		roleExists, err := RoleExists(ctx, p.InternalSQLTxn(), oldRole)
		if err != nil {
			return nil, err
		}
		if !roleExists {
			return nil, sqlerrors.NewUndefinedUserError(oldRole)
		}
	}
	newRole, err := decodeusername.FromRoleSpec(
		p.SessionData(), username.PurposeValidation, n.NewRole,
	)
	if err != nil {
		return nil, err
	}
	roleExists, err := RoleExists(ctx, p.InternalSQLTxn(), newRole)
	if !roleExists {
		return nil, sqlerrors.NewUndefinedUserError(newRole)
	}
	if err != nil {
		return nil, err
	}

	hasAdminRole, err := p.HasAdminRole(ctx)
	if err != nil {
		return nil, err
	}

	// The current user must either be an admin or we have to check that
	// the current user is a member of both the new roles and all the
	// old roles.
	if !hasAdminRole {
		memberOf, err := p.MemberOfWithAdminOption(ctx, p.User())
		if err != nil {
			return nil, err
		}
		if p.User() != newRole {
			if _, ok := memberOf[newRole]; !ok {
				return nil, errors.WithHint(
					pgerror.Newf(pgcode.InsufficientPrivilege,
						"permission denied to reassign objects"),
					"user must be a member of the new role")
			}
		}
		for _, oldRole := range normalizedOldRoles {
			if p.User() != oldRole {
				if _, ok := memberOf[oldRole]; !ok {
					return nil, errors.WithHint(
						pgerror.Newf(pgcode.InsufficientPrivilege,
							"permission denied to reassign objects"),
						"user must be a member of the old roles")
				}
			}
		}
	}
	return &reassignOwnedByNode{n: n, normalizedOldRoles: normalizedOldRoles}, nil
}

func (n *reassignOwnedByNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.CreateReassignOwnedByCounter())

	all, err := params.p.Descriptors().GetAllDescriptors(params.ctx, params.p.txn)
	if err != nil {
		return err
	}

	// Filter for all objects in current database.
	currentDatabase := params.p.CurrentDatabase()
	currentDbDesc, err := params.p.Descriptors().MutableByName(params.p.txn).Database(params.ctx, currentDatabase)
	if err != nil {
		return err
	}

	lCtx := newInternalLookupCtx(all.OrderedDescriptors(), currentDbDesc.ImmutableCopy().(catalog.DatabaseDescriptor))

	// Iterate through each object, check for ownership by an old role.
	for _, oldRole := range n.normalizedOldRoles {
		// There should only be one database (current).
		for _, dbID := range lCtx.dbIDs {
			isOwner, err := isOwner(params.ctx, params.p, lCtx.dbDescs[dbID], oldRole)
			if err != nil {
				return err
			}
			if isOwner {
				if err := n.reassignDatabaseOwner(lCtx.dbDescs[dbID], params); err != nil {
					return err
				}
			}
		}
		for _, schemaID := range lCtx.schemaIDs {
			isOwner, err := isOwner(params.ctx, params.p, lCtx.schemaDescs[schemaID], oldRole)
			if err != nil {
				return err
			}
			if isOwner {
				// Don't reassign public schema.
				// TODO(richardjcai): revisit this in 22.2, in 22.1 we do not allow
				// modifying the public schema.
				if lCtx.schemaDescs[schemaID].GetName() == tree.PublicSchema {
					continue
				}
				if err := n.reassignSchemaOwner(lCtx.schemaDescs[schemaID], currentDbDesc, params); err != nil {
					return err
				}
			}
		}

		for _, tbID := range lCtx.tbIDs {
			isOwner, err := isOwner(params.ctx, params.p, lCtx.tbDescs[tbID], oldRole)
			if err != nil {
				return err
			}
			if isOwner {
				if err := n.reassignTableOwner(lCtx.tbDescs[tbID], params); err != nil {
					return err
				}
			}
		}
		for _, typID := range lCtx.typIDs {
			isOwner, err := isOwner(params.ctx, params.p, lCtx.typDescs[typID], oldRole)
			if err != nil {
				return err
			}
			if isOwner && (lCtx.typDescs[typID].AsAliasTypeDescriptor() == nil) {
				if err := n.reassignTypeOwner(lCtx.typDescs[typID].(catalog.NonAliasTypeDescriptor), params); err != nil {
					return err
				}
			}
		}
		for _, fnID := range lCtx.fnIDs {
			isOwner, err := isOwner(params.ctx, params.p, lCtx.fnDescs[fnID], oldRole)
			if err != nil {
				return err
			}
			if isOwner {
				if err := n.reassignFunctionOwner(lCtx.fnDescs[fnID], params); err != nil {
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
	mutableDbDesc, err := params.p.Descriptors().MutableByID(params.p.txn).Desc(params.ctx, dbDesc.GetID())
	if err != nil {
		return err
	}
	if mutableDbDesc.Dropped() {
		return nil
	}
	owner, err := decodeusername.FromRoleSpec(
		params.p.SessionData(), username.PurposeValidation, n.n.NewRole,
	)
	if err != nil {
		return err
	}
	if err := params.p.setNewDatabaseOwner(params.ctx, mutableDbDesc, owner); err != nil {
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
	schemaDesc catalog.SchemaDescriptor, dbDesc *dbdesc.Mutable, params runParams,
) error {
	mutableSchemaDesc, err := params.p.Descriptors().MutableByID(params.p.txn).Desc(params.ctx, schemaDesc.GetID())
	if err != nil {
		return err
	}
	if mutableSchemaDesc.Dropped() {
		return nil
	}
	owner, err := decodeusername.FromRoleSpec(
		params.p.SessionData(), username.PurposeValidation, n.n.NewRole,
	)
	if err != nil {
		return err
	}
	if err := params.p.setNewSchemaOwner(
		params.ctx, dbDesc, mutableSchemaDesc.(*schemadesc.Mutable), owner); err != nil {
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
	mutableTbDesc, err := params.p.Descriptors().MutableByID(params.p.txn).Desc(params.ctx, tbDesc.GetID())
	if err != nil {
		return err
	}
	if mutableTbDesc.Dropped() {
		return nil
	}
	tableName, err := params.p.getQualifiedTableName(params.ctx, tbDesc)
	if err != nil {
		return err
	}

	owner, err := decodeusername.FromRoleSpec(
		params.p.SessionData(), username.PurposeValidation, n.n.NewRole,
	)
	if err != nil {
		return err
	}
	if err := params.p.setNewTableOwner(
		params.ctx, mutableTbDesc.(*tabledesc.Mutable), *tableName, owner); err != nil {
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
	typDesc catalog.NonAliasTypeDescriptor, params runParams,
) error {
	mutableTypDesc, err := params.p.Descriptors().MutableByID(params.p.txn).Desc(params.ctx, typDesc.GetID())
	if err != nil {
		return err
	}
	if mutableTypDesc.Dropped() {
		return nil
	}
	arrayDesc, err := params.p.Descriptors().MutableByID(params.p.txn).Type(params.ctx, typDesc.GetArrayTypeID())
	if err != nil {
		return err
	}

	typeName, err := params.p.getQualifiedTypeName(params.ctx, mutableTypDesc.(*typedesc.Mutable))
	if err != nil {
		return err
	}
	arrayTypeName, err := params.p.getQualifiedTypeName(params.ctx, arrayDesc)
	if err != nil {
		return err
	}

	owner, err := decodeusername.FromRoleSpec(
		params.p.SessionData(), username.PurposeValidation, n.n.NewRole,
	)
	if err != nil {
		return err
	}
	if err := params.p.setNewTypeOwner(
		params.ctx, mutableTypDesc.(*typedesc.Mutable), arrayDesc, *typeName,
		*arrayTypeName, owner); err != nil {
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

func (n *reassignOwnedByNode) reassignFunctionOwner(
	fnDesc catalog.FunctionDescriptor, params runParams,
) error {
	mutableDesc, err := params.p.Descriptors().MutableByID(params.p.txn).Function(params.ctx, fnDesc.GetID())
	if err != nil {
		return err
	}
	if mutableDesc.Dropped() {
		return nil
	}
	newOwner, err := decodeusername.FromRoleSpec(
		params.p.SessionData(), username.PurposeValidation, n.n.NewRole,
	)
	if err != nil {
		return err
	}
	mutableDesc.GetPrivileges().SetOwner(newOwner)
	return params.p.writeFuncSchemaChange(params.ctx, mutableDesc)
}

func (n *reassignOwnedByNode) Next(runParams) (bool, error) { return false, nil }
func (n *reassignOwnedByNode) Values() tree.Datums          { return tree.Datums{} }
func (n *reassignOwnedByNode) Close(context.Context)        {}
