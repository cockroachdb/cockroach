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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// alterRoleOrUserNode represents an ALTER ROLE ... [WITH] OPTION... statement.
type alterRoleOrUserNode struct {
	userAuthInfo
	roleOptions roleoption.RoleOptionList

	run alterRoleOrUserRun
}

// AlterRoleOptions alters a user's permissios
func (p *planner) AlterRoleOrUserOptions(
	ctx context.Context, n *tree.AlterRoleOrUserOptions,
) (*alterRoleOrUserNode, error) {
	// Note that for Postgres, only superuser can ALTER another superuser.
	// CockroachDB does not support superuser privilege right now.
	// However we make it so the admin role cannot be edited (done in startExec).
	if err := p.HasRolePrivilege(ctx, roleoption.CREATEROLE); err != nil {
		return nil, err
	}

	roleOptions, err := n.RoleOptions.Convert(p.TypeAsString, "ALTER ROLE OR USER")
	if err != nil {
		return nil, err
	}
	if err := roleOptions.CheckRoleOptionConflicts(); err != nil {
		return nil, err
	}

	ua, err := p.getUserAuthInfo(n.Name, nil, "ALTER ROLE OR USER")
	if err != nil {
		return nil, err
	}

	return &alterRoleOrUserNode{
		userAuthInfo: ua,
		roleOptions:  roleOptions,
	}, nil
}

// alterRoleOrUserRun is the run-time state of
// alterRoleOrUserNode for local execution.
type alterRoleOrUserRun struct {
	rowsAffected int
}

func (n *alterRoleOrUserNode) startExec(params runParams) error {
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

	normalizedUsername, _, err := n.userAuthInfo.resolve()
	if err != nil {
		return err
	}

	setStmt, err := n.roleOptions.CreateSetStmt()
	if err != nil {
		return err
	}

	stmt := fmt.Sprintf(`UPDATE %s SET %s WHERE username = $1`, userTableName, setStmt)

	n.run.rowsAffected, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
		params.ctx,
		"update-role",
		params.p.txn,
		stmt,
		normalizedUsername,
	)

	if err != nil {
		return err
	}
	if n.run.rowsAffected == 0 {
		return pgerror.Newf(pgcode.UndefinedObject,
			"user %s does not exist", name)
	}
	return nil
}

func (*alterRoleOrUserNode) Next(runParams) (bool, error) { return false, nil }
func (*alterRoleOrUserNode) Values() tree.Datums          { return tree.Datums{} }
func (*alterRoleOrUserNode) Close(context.Context)        {}

func (n *alterRoleOrUserNode) FastPathResults() (int, bool) {
	return n.run.rowsAffected, true
}
