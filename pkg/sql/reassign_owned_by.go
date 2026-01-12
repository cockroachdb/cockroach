// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// ReassignOwnedByNode represents a REASSIGN OWNED BY <role(s)> TO <role> statement.
type reassignOwnedByNode struct {
	zeroInputPlanNode
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
		if err := p.CheckRoleExists(ctx, oldRole); err != nil {
			return nil, err
		}
	}
	newRole, err := decodeusername.FromRoleSpec(
		p.SessionData(), username.PurposeValidation, n.NewRole,
	)
	if err != nil {
		return nil, err
	}
	if err := p.CheckRoleExists(ctx, newRole); err != nil {
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

func (n *reassignOwnedByNode) StartExec(params runParams) error {
	telemetry.Inc(sqltelemetry.CreateReassignOwnedByCounter())

	all, err := params.P.(*planner).Descriptors().GetAllDescriptors(params.Ctx, params.P.(*planner).txn)
	if err != nil {
		return err
	}

	// Filter for all objects in current database.
	currentDatabase := params.P.(*planner).CurrentDatabase()
	currentDbDesc, err := params.P.(*planner).Descriptors().MutableByName(params.P.(*planner).txn).Database(params.Ctx, currentDatabase)
	if err != nil {
		return err
	}

	lCtx := newInternalLookupCtx(all.OrderedDescriptors(), currentDbDesc.ImmutableCopy().(catalog.DatabaseDescriptor))

	// Iterate through each object, check for ownership by an old role.
	for _, oldRole := range n.normalizedOldRoles {
		// There should only be one database (current).
		for _, dbID := range lCtx.dbIDs {
			dbDesc := lCtx.dbDescs[dbID]
			owner, err := params.P.(*planner).getOwnerOfPrivilegeObject(params.Ctx, dbDesc)
			if err != nil {
				return err
			}
			if owner == oldRole {
				if err := n.reassignDatabaseOwner(dbDesc, params); err != nil {
					return err
				}
			}
		}
		for _, schemaID := range lCtx.schemaIDs {
			schemaDesc := lCtx.schemaDescs[schemaID]
			owner, err := params.P.(*planner).getOwnerOfPrivilegeObject(params.Ctx, schemaDesc)
			if err != nil {
				return err
			}
			if owner == oldRole {
				// Don't reassign the descriptorless public schema for the system
				// database.
				if schemaID == keys.SystemPublicSchemaID {
					continue
				}
				if err := n.reassignSchemaOwner(lCtx.schemaDescs[schemaID], currentDbDesc, params); err != nil {
					return err
				}
			}
		}

		for _, tbID := range lCtx.tbIDs {
			tbDesc := lCtx.tbDescs[tbID]
			owner, err := params.P.(*planner).getOwnerOfPrivilegeObject(params.Ctx, tbDesc)
			if err != nil {
				return err
			}
			if owner == oldRole {
				if err := n.reassignTableOwner(lCtx.tbDescs[tbID], params); err != nil {
					return err
				}
			}
		}
		for _, typID := range lCtx.typIDs {
			typDesc := lCtx.typDescs[typID]
			owner, err := params.P.(*planner).getOwnerOfPrivilegeObject(params.Ctx, typDesc)
			if err != nil {
				return err
			}
			if owner == oldRole && (lCtx.typDescs[typID].AsAliasTypeDescriptor() == nil) {
				if err := n.reassignTypeOwner(lCtx.typDescs[typID].(catalog.NonAliasTypeDescriptor), params); err != nil {
					return err
				}
			}
		}
		for _, fnID := range lCtx.fnIDs {
			fnDesc := lCtx.fnDescs[fnID]
			owner, err := params.P.(*planner).getOwnerOfPrivilegeObject(params.Ctx, fnDesc)
			if err != nil {
				return err
			}
			if owner == oldRole {
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
	mutableDbDesc, err := params.P.(*planner).Descriptors().MutableByID(params.P.(*planner).txn).Desc(params.Ctx, dbDesc.GetID())
	if err != nil {
		return err
	}
	if mutableDbDesc.Dropped() {
		return nil
	}
	owner, err := decodeusername.FromRoleSpec(
		params.P.(*planner).SessionData(), username.PurposeValidation, n.n.NewRole,
	)
	if err != nil {
		return err
	}
	if err := params.P.(*planner).setNewDatabaseOwner(params.Ctx, mutableDbDesc, owner); err != nil {
		return err
	}
	if err := params.P.(*planner).writeNonDropDatabaseChange(
		params.Ctx,
		mutableDbDesc.(*dbdesc.Mutable),
		tree.AsStringWithFQNames(n.n, params.P.(*planner).Ann()),
	); err != nil {
		return err
	}
	return nil
}

func (n *reassignOwnedByNode) reassignSchemaOwner(
	schemaDesc catalog.SchemaDescriptor, dbDesc *dbdesc.Mutable, params runParams,
) error {
	mutableSchemaDesc, err := params.P.(*planner).Descriptors().MutableByID(params.P.(*planner).txn).Desc(params.Ctx, schemaDesc.GetID())
	if err != nil {
		return err
	}
	if mutableSchemaDesc.Dropped() {
		return nil
	}
	owner, err := decodeusername.FromRoleSpec(
		params.P.(*planner).SessionData(), username.PurposeValidation, n.n.NewRole,
	)
	if err != nil {
		return err
	}
	if err := params.P.(*planner).setNewSchemaOwner(
		params.Ctx, dbDesc, mutableSchemaDesc.(*schemadesc.Mutable), owner); err != nil {
		return err
	}
	if err := params.P.(*planner).writeSchemaDescChange(params.Ctx,
		mutableSchemaDesc.(*schemadesc.Mutable),
		tree.AsStringWithFQNames(n.n, params.P.(*planner).Ann()),
	); err != nil {
		return err
	}
	return nil
}

func (n *reassignOwnedByNode) reassignTableOwner(
	tbDesc catalog.TableDescriptor, params runParams,
) error {
	mutableTbDesc, err := params.P.(*planner).Descriptors().MutableByID(params.P.(*planner).txn).Desc(params.Ctx, tbDesc.GetID())
	if err != nil {
		return err
	}
	if mutableTbDesc.Dropped() {
		return nil
	}
	tableName, err := params.P.(*planner).getQualifiedTableName(params.Ctx, tbDesc)
	if err != nil {
		return err
	}

	owner, err := decodeusername.FromRoleSpec(
		params.P.(*planner).SessionData(), username.PurposeValidation, n.n.NewRole,
	)
	if err != nil {
		return err
	}
	if err := params.P.(*planner).setNewTableOwner(
		params.Ctx, mutableTbDesc.(*tabledesc.Mutable), *tableName, owner); err != nil {
		return err
	}
	if err := params.P.(*planner).writeSchemaChange(
		params.Ctx, mutableTbDesc.(*tabledesc.Mutable), descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}
	return nil
}

func (n *reassignOwnedByNode) reassignTypeOwner(
	typDesc catalog.NonAliasTypeDescriptor, params runParams,
) error {
	mutableTypDesc, err := params.P.(*planner).Descriptors().MutableByID(params.P.(*planner).txn).Desc(params.Ctx, typDesc.GetID())
	if err != nil {
		return err
	}
	if mutableTypDesc.Dropped() {
		return nil
	}
	arrayDesc, err := params.P.(*planner).Descriptors().MutableByID(params.P.(*planner).txn).Type(params.Ctx, typDesc.GetArrayTypeID())
	if err != nil {
		return err
	}

	typeName, err := params.P.(*planner).getQualifiedTypeName(params.Ctx, mutableTypDesc.(*typedesc.Mutable))
	if err != nil {
		return err
	}
	arrayTypeName, err := params.P.(*planner).getQualifiedTypeName(params.Ctx, arrayDesc)
	if err != nil {
		return err
	}

	owner, err := decodeusername.FromRoleSpec(
		params.P.(*planner).SessionData(), username.PurposeValidation, n.n.NewRole,
	)
	if err != nil {
		return err
	}
	if err := params.P.(*planner).setNewTypeOwner(
		params.Ctx, mutableTypDesc.(*typedesc.Mutable), arrayDesc, *typeName,
		*arrayTypeName, owner); err != nil {
		return err
	}
	if err := params.P.(*planner).writeTypeSchemaChange(
		params.Ctx, mutableTypDesc.(*typedesc.Mutable), tree.AsStringWithFQNames(n.n, params.P.(*planner).Ann()),
	); err != nil {
		return err
	}
	if err := params.P.(*planner).writeTypeSchemaChange(
		params.Ctx, arrayDesc, tree.AsStringWithFQNames(n.n, params.P.(*planner).Ann()),
	); err != nil {
		return err
	}
	return nil
}

func (n *reassignOwnedByNode) reassignFunctionOwner(
	fnDesc catalog.FunctionDescriptor, params runParams,
) error {
	mutableDesc, err := params.P.(*planner).Descriptors().MutableByID(params.P.(*planner).txn).Function(params.Ctx, fnDesc.GetID())
	if err != nil {
		return err
	}
	if mutableDesc.Dropped() {
		return nil
	}
	newOwner, err := decodeusername.FromRoleSpec(
		params.P.(*planner).SessionData(), username.PurposeValidation, n.n.NewRole,
	)
	if err != nil {
		return err
	}
	mutableDesc.GetPrivileges().SetOwner(newOwner)
	return params.P.(*planner).writeFuncSchemaChange(params.Ctx, mutableDesc)
}

func (n *reassignOwnedByNode) Next(runParams) (bool, error) { return false, nil }
func (n *reassignOwnedByNode) Values() tree.Datums          { return tree.Datums{} }
func (n *reassignOwnedByNode) Close(context.Context)        {}
