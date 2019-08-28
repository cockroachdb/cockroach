// Copyright 2017 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/pkg/errors"
)

// DropUserNode deletes entries from the system.users table.
// This is called from DROP USER and DROP ROLE.
type DropUserNode struct {
	ifExists bool
	isRole   bool
	names    func() ([]string, error)

	run dropUserRun
}

// DropUser drops a list of users.
// Privileges: DELETE on system.users.
func (p *planner) DropUser(ctx context.Context, n *tree.DropUser) (planNode, error) {
	return p.DropUserNode(ctx, n.Names, n.IfExists, false /* isRole */, "DROP USER")
}

// DropUserNode creates a "drop user" plan node. This can be called from DROP USER or DROP ROLE.
func (p *planner) DropUserNode(
	ctx context.Context, namesE tree.Exprs, ifExists bool, isRole bool, opName string,
) (*DropUserNode, error) {
	tDesc, err := ResolveExistingObject(ctx, p, userTableName, tree.ObjectLookupFlagsWithRequired(), ResolveRequireTableDesc)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, tDesc, privilege.DELETE); err != nil {
		return nil, err
	}

	names, err := p.TypeAsStringArray(namesE, opName)
	if err != nil {
		return nil, err
	}

	return &DropUserNode{
		ifExists: ifExists,
		isRole:   isRole,
		names:    names,
	}, nil
}

// dropUserRun contains the run-time state of DropUserNode during local execution.
type dropUserRun struct {
	// The number of users deleted.
	numDeleted int
}

func (n *DropUserNode) startExec(params runParams) error {
	var entryType string
	if n.isRole {
		entryType = "role"
	} else {
		entryType = "user"
	}

	names, err := n.names()
	if err != nil {
		return err
	}

	userNames := make(map[string]struct{})
	for _, name := range names {
		normalizedUsername, err := NormalizeAndValidateUsername(name)
		if err != nil {
			return err
		}
		userNames[normalizedUsername] = struct{}{}
	}

	f := tree.NewFmtCtx(tree.FmtSimple)
	defer f.Close()

	// Now check whether the user still has permission on any object in the database.

	// First check all the databases.
	if err := forEachDatabaseDesc(params.ctx, params.p, nil, /*nil prefix = all databases*/
		func(db *sqlbase.DatabaseDescriptor) error {
			for _, u := range db.GetPrivileges().Users {
				if _, ok := userNames[u.User]; ok {
					if f.Len() > 0 {
						f.WriteString(", ")
					}
					f.FormatNameP(&db.Name)
					break
				}
			}
			return nil
		}); err != nil {
		return err
	}

	// Then check all the tables.
	//
	// We need something like forEachTableAll here, however we can't use
	// the predefined forEachTableAll() function because we need to look
	// at all _visible_ descriptors, not just those on which the current
	// user has permission.
	descs, err := params.p.Tables().getAllDescriptors(params.ctx, params.p.txn)
	if err != nil {
		return err
	}
	lCtx := newInternalLookupCtx(descs, nil /*prefix - we want all descriptors */)
	for _, tbID := range lCtx.tbIDs {
		table := lCtx.tbDescs[tbID]
		if !tableIsVisible(table, true /*allowAdding*/) {
			continue
		}
		for _, u := range table.GetPrivileges().Users {
			if _, ok := userNames[u.User]; ok {
				if f.Len() > 0 {
					f.WriteString(", ")
				}
				parentName := lCtx.getParentName(table)
				tn := tree.MakeTableName(tree.Name(parentName), tree.Name(table.Name))
				f.FormatNode(&tn)
				break
			}
		}
	}

	// Was there any object dependin on that user?
	if f.Len() > 0 {
		fnl := tree.NewFmtCtx(tree.FmtSimple)
		defer fnl.Close()
		for i, name := range names {
			if i > 0 {
				fnl.WriteString(", ")
			}
			fnl.FormatName(name)
		}
		return pgerror.Newf(pgcode.Grouping,
			"cannot drop user%s or role%s %s: grants still exist on %s",
			util.Pluralize(int64(len(names))), util.Pluralize(int64(len(names))),
			fnl.String(), f.String(),
		)
	}

	// All safe - do the work.
	var numUsersDeleted, numRoleMembershipsDeleted int
	for normalizedUsername := range userNames {
		// Specifically reject special users and roles. Some (root, admin) would fail with
		// "privileges still exist" first.
		if normalizedUsername == sqlbase.AdminRole || normalizedUsername == sqlbase.PublicRole {
			return pgerror.Newf(
				pgcode.InvalidParameterValue, "cannot drop special role %s", normalizedUsername)
		}
		if normalizedUsername == security.RootUser {
			return pgerror.Newf(
				pgcode.InvalidParameterValue, "cannot drop special user %s", normalizedUsername)
		}

		rowsAffected, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			"drop-user",
			params.p.txn,
			`DELETE FROM system.users WHERE username=$1 AND "isRole" = $2`,
			normalizedUsername,
			n.isRole,
		)
		if err != nil {
			return err
		}

		if rowsAffected == 0 && !n.ifExists {
			return errors.Errorf("%s %s does not exist", entryType, normalizedUsername)
		}
		numUsersDeleted += rowsAffected

		// Drop all role memberships involving the user/role.
		rowsAffected, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			"drop-role-membership",
			params.p.txn,
			`DELETE FROM system.role_members WHERE "role" = $1 OR "member" = $1`,
			normalizedUsername,
		)
		if err != nil {
			return err
		}

		numRoleMembershipsDeleted += rowsAffected
	}

	if numRoleMembershipsDeleted > 0 {
		// Some role memberships have been deleted, bump role_members table version to
		// force a refresh of role membership.
		if err := params.p.BumpRoleMembershipTableVersion(params.ctx); err != nil {
			return err
		}
	}

	n.run.numDeleted = numUsersDeleted

	return nil
}

// Next implements the planNode interface.
func (*DropUserNode) Next(runParams) (bool, error) { return false, nil }

// Values implements the planNode interface.
func (*DropUserNode) Values() tree.Datums { return tree.Datums{} }

// Close implements the planNode interface.
func (*DropUserNode) Close(context.Context) {}

// FastPathResults implements the planNodeFastPath interface.
func (n *DropUserNode) FastPathResults() (int, bool) { return n.run.numDeleted, true }
