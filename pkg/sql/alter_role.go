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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/roleprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// alterRoleNode represents an ALTER ROLE ... [WITH] OPTION... statement.
type alterRoleNode struct {
	name           func() (string, error)
	rolePrivileges roleprivilege.List

	run alterRoleRun
}

// AlterRolePrivileges alters a user's permissios
func (p *planner) AlterRolePrivileges(
	ctx context.Context, n *tree.AlterRolePrivileges,
) (*alterRoleNode, error) {
	// Note that for Postgres, only superuser can ALTER another superuser.
	// CockroachDB does not support superuser privilege right now.
	// However we make it so the admin role cannot be edited (done in startExec).
	if err := p.HasRolePrivilege(ctx, roleprivilege.CREATEROLE); err != nil {
		return nil, err
	}

	if err := n.RolePrivileges.CheckRolePrivilegeConflicts(); err != nil {
		return nil, err
	}

	name, err := p.TypeAsString(n.Name, "ALTER ROLE")
	if err != nil {
		return nil, err
	}

	return &alterRoleNode{
		name:           name,
		rolePrivileges: n.RolePrivileges,
	}, nil
}

// alterRoleRun is the run-time state of
// alterRoleNode for local execution.
type alterRoleRun struct {
	rowsAffected int
}

func (n *alterRoleNode) startExec(params runParams) error {
	name, err := n.name()
	if err != nil {
		return err
	}
	if name == "" {
		return errNoUserNameSpecified
	}
	if name == "admin" {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"cannot edit admin role")
	}
	normalizedUsername, err := NormalizeAndValidateUsername(name)
	if err != nil {
		return err
	}

	// Check if role exists.
	row, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.QueryRowEx(
		params.ctx,
		"update-role",
		params.p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf("SELECT 1 FROM %s WHERE username = $1", userTableName),
		normalizedUsername,
	)
	if err != nil {
		return err
	}
	if row == nil {
		return errors.Newf("role %s does not exist", normalizedUsername)
	}

	stmts, err := n.rolePrivileges.GetSQLStmts()
	if err != nil {
		return err
	}

	for _, stmt := range stmts {
		_, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			"update-role",
			params.p.txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			stmt,
			normalizedUsername,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (*alterRoleNode) Next(runParams) (bool, error) { return false, nil }
func (*alterRoleNode) Values() tree.Datums          { return tree.Datums{} }
func (*alterRoleNode) Close(context.Context)        {}

func (n *alterRoleNode) FastPathResults() (int, bool) {
	return n.run.rowsAffected, true
}
