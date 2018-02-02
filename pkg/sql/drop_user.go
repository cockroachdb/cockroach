// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
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
	tDesc, err := getTableDesc(ctx, p.txn, p.getVirtualTabler(), userTableName)
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

	f := tree.NewFmtCtxWithBuf(tree.FmtSimple)
	defer f.Close()

	if err := forEachDatabaseDesc(params.ctx, params.p,
		func(db *sqlbase.DatabaseDescriptor) error {
			for _, u := range db.GetPrivileges().Users {
				if _, ok := userNames[u.User]; ok {
					if f.Len() > 0 {
						f.WriteString(", ")
					}
					f.FormatNameP(&db.Name)
				}
			}
			return nil
		}); err != nil {
		return err
	}
	if err := forEachTableDescAll(params.ctx, params.p, "",
		func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
			for _, u := range table.GetPrivileges().Users {
				if _, ok := userNames[u.User]; ok {
					if f.Len() > 0 {
						f.WriteString(", ")
					}
					tn := tree.MakeTableName(tree.Name(db.Name), tree.Name(table.Name))
					f.FormatNode(&tn)
				}
			}
			return nil
		}); err != nil {
		return err
	}
	if f.Len() > 0 {
		fnl := tree.NewFmtCtxWithBuf(tree.FmtSimple)
		defer fnl.Close()
		for i, name := range names {
			if i > 0 {
				fnl.WriteString(", ")
			}
			fnl.FormatName(name)
		}
		return pgerror.NewErrorf(pgerror.CodeGroupingError,
			"cannot drop user%s or role%s %s: grants still exist on %s",
			util.Pluralize(int64(len(names))), util.Pluralize(int64(len(names))),
			fnl.String(), f.String(),
		)
	}

	numDeleted := 0
	for normalizedUsername := range userNames {
		// We don't specifically check whether it's a user or role, we should never reach
		// this point anyway, the "privileges exist" check will fail first.
		if normalizedUsername == sqlbase.AdminRole {
			return errors.Errorf("role %s cannot be dropped", sqlbase.AdminRole)
		}
		if normalizedUsername == security.RootUser {
			return errors.Errorf("user %s cannot be dropped", security.RootUser)
		}

		internalExecutor := InternalExecutor{ExecCfg: params.extendedEvalCtx.ExecCfg}
		rowsAffected, err := internalExecutor.ExecuteStatementInTransaction(
			params.ctx,
			"drop-user",
			params.p.txn,
			`DELETE FROM system.public.users WHERE username=$1 AND "isRole" = $2`,
			normalizedUsername,
			n.isRole,
		)
		if err != nil {
			return err
		}

		if rowsAffected == 0 && !n.ifExists {
			return errors.Errorf("%s %s does not exist", entryType, normalizedUsername)
		}
		numDeleted += rowsAffected

		// Drop all role memberships involving the user/role.
		_, err = internalExecutor.ExecuteStatementInTransaction(
			params.ctx,
			"drop-role-membership",
			params.p.txn,
			`DELETE FROM system.public.role_members WHERE "role" = $1 OR "member" = $1`,
			normalizedUsername,
		)
		if err != nil {
			return err
		}
	}

	n.run.numDeleted = numDeleted

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
