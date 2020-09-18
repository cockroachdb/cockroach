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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// ReassignOwnedByNode represents a REASSIGN OWNED BY <name> TO <name> statement.
// TODO(angelaw): to implement
type reassignOwnedByNode struct {
	n *tree.ReassignOwnedBy
}

func (p *planner) ReassignOwnedBy(ctx context.Context, n *tree.ReassignOwnedBy) (planNode, error) {

	// Check all roles in old roles exist
	// And that current user has membership on old role (PostgresQL requirement)
	// TODO(angelaw): Refactor the error checking out to authorization?
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
				// TODO(angelaw): find error message from psql
				pgcode.InsufficientPrivilege, "current user must be member of role %q", oldRole)
		}
	}

	// Check new role exists
	roleExists, err := p.RoleExists(ctx, n.NewRole)
	if err != nil {
		return nil, err
	}

	if !roleExists {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "role/user %q does not exist", n.NewRole)
	}

	// And that current user has membership on new role (PostgresQL requirement)
	if _, ok := memberOf[n.NewRole]; !ok {
		return nil, pgerror.Newf(
			// TODO(angelaw): find error message from psql
			pgcode.InsufficientPrivilege, "current user must be member of role %q", n.NewRole)
	}

	// Find all objects in current database - use getAllDescriptors, newInternalLookupCtxFromDescriptors
	allDescs, err := p.Descriptors().GetAllDescriptors(ctx, p.txn)
	if err != nil {
		return nil, err
	}

	currentDatabase := p.CurrentDatabase()
	currentDbDesc, err := p.ResolveMutableDatabaseDescriptor(ctx, currentDatabase, true)
	if err != nil {
		return nil, err
	}

	// TODO(angelaw): This is messy but we need an immutableDbDesc for description and
	// we only have a ResolveMutableDatabaseDescriptor — any better ideas?
	lCtx := newInternalLookupCtx(ctx, allDescs, currentDbDesc.ImmutableCopy().(*dbdesc.Immutable))

	// For each role in old roles
	for _, oldRole := range n.OldRoles {
		// Iterate through each object in current DB
		for _, dbDesc := range lCtx.dbDescs {
			// We only expect 1 matching db (current database)
			if p.IsOwner(dbDesc, oldRole) {
				p.AlterDatabaseOwner(ctx, tree.AlterDatabaseOwner) // Does not include new owner string
			}
		}
		for _, schemaDesc := range lCtx.schemaDescs {
			if p.IsOwner(schemaDesc, oldRole) {
				// What is job desc here?
				p.alterSchemaOwner(ctx, dbDesc, schemaDesc, n.NewRole, jobDesc)
			}
		}

		for _, tbDesc := range lCtx.tbDescs {
			if p.IsOwner(tbDesc, oldRole) {
				// Need to populate alterTableNode?
				p.alterTableOwner(ctx, alterTableNode, n.NewRole)
			}
		}

		for _, typDesc := range lCtx.typDescs {
			if p.IsOwner(typDesc, oldRole) {
				// Need to populate alterTypeNode?
				p.alterTypeOwner(ctx, alterTypeNode, n.NewRole)
			}
		}
	}

	// Not sure we can directly call those methods as checkCanAlterToNewOwner is called and has different checks
	// I think it actually works fine as long as we pass in hasOwnership = true

	return nil, unimplemented.NewWithIssue(52022, "reassign owned by is not yet implemented")
}
