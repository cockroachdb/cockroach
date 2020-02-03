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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// alterRoleOrUserNode represents an ALTER ROLE ... [WITH] OPTION... statement.
type alterRoleOrUserNode struct {
	userAuthInfo
	roleOptions tree.RoleOptionList

	run alterRoleOrUserRun
}

// AlterRoleOptions alters a user's permissios
func (p *planner) AlterRoleOrUserOptions(
	ctx context.Context, n *tree.AlterRoleOrUserOptions,
) (*alterRoleOrUserNode, error) {
	// Note that for Postgres, only superuser can ALTER another superuser.
	// CockroachDB does not support superuser privilege right now.
	// However we make it so the admin role cannot be edited (done in startExec).
	if err := p.HasRolePrivilege(ctx, tree.CREATEROLE); err != nil {
		return nil, err
	}

	if err := n.RoleOptions.CheckRoleOptionConflicts(); err != nil {
		return nil, err
	}

	passwordE := tree.Expr(nil)
	for _, ro := range n.RoleOptions {
		if ro.Option == tree.PASSWORD {
			passwordE = ro.Value
		}
	}

	ua, err := p.getUserAuthInfo(n.Name, passwordE, "ALTER ROLE OR USER")
	if err != nil {
		return nil, err
	}

	return &alterRoleOrUserNode{
		userAuthInfo: ua,
		roleOptions:  n.RoleOptions,
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

	normalizedUsername, hashedPassword, err := n.userAuthInfo.resolve()
	if err != nil {
		return err
	}

	// TODO(knz): Remove in 20.2.
	if normalizedUsername == security.RootUser && len(hashedPassword) > 0 &&
		!cluster.Version.IsActive(params.ctx, params.EvalContext().Settings, cluster.VersionRootPassword) {
		return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			`setting a root password requires all nodes to be upgraded to %s`,
			cluster.VersionByKey(cluster.VersionRootPassword),
		)
	}

	if len(hashedPassword) > 0 && params.extendedEvalCtx.ExecCfg.RPCContext.Insecure {
		// We disallow setting a non-empty password in insecure mode
		// because insecure means an observer may have MITM'ed the change
		// and learned the password.
		//
		// It's valid to clear the password (WITH PASSWORD NULL) however
		// since that forces cert auth when moving back to secure mode,
		// and certs can't be MITM'ed over the insecure SQL connection.
		return pgerror.New(pgcode.InvalidPassword,
			"setting or updating a password is not supported in insecure mode")
	}

	setStmt, err := n.CreateSetStmt(hashedPassword)
	if err != nil {
		return err
	}

	// 1. Only setting role options excluding password.
	passwordClause := ""
	// 2. Only setting password.
	if n.roleOptions.Contains(tree.PASSWORD) {
		passwordClause = fmt.Sprintf("\"%s\" = $2", tree.PASSWORD.ToSQLColumnName())
		// 3. Setting both password and other role options.
		if setStmt != "" {
			passwordClause = fmt.Sprintf("\"%s\" = $2, ", tree.PASSWORD.ToSQLColumnName())
		}
	} else {
		stmt := fmt.Sprintf(`UPDATE %s SET %s WHERE username = $1`, userTableName, setStmt)

		n.run.rowsAffected, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			"update-role",
			params.p.txn,
			stmt,
			normalizedUsername,
		)
	}

	stmt := fmt.Sprintf(`UPDATE %s SET %s %s WHERE username = $1`, userTableName, passwordClause, setStmt)

	n.run.rowsAffected, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
		params.ctx,
		"update-role",
		params.p.txn,
		stmt,
		normalizedUsername,
		hashedPassword,
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

// CreateSetStmtFromRoleOptions returns a string of the form:
// "SET "optionA" = true, "optionB" = false".
func (n *alterRoleOrUserNode) CreateSetStmt() (string, error) {
	if len(n.roleOptions) <= 0 {
		return "", pgerror.Newf(pgcode.Syntax, "no role options found")
	}
	setStmt := ""
	for i, roleOption := range n.roleOptions {
		option := roleOption.Option
		// Password is a special case
		if option == tree.PASSWORD {

		} else {
			format := ", \"%s\" = %s "
			if i == 0 {
				format = "\"%s\" = %s "
			}
			setStmt += fmt.Sprintf(format, option.ToSQLColumnName(), roleOption.ToSQLValue())
		}
	}

	return setStmt, nil
}

func (*alterRoleOrUserNode) Next(runParams) (bool, error) { return false, nil }
func (*alterRoleOrUserNode) Values() tree.Datums          { return tree.Datums{} }
func (*alterRoleOrUserNode) Close(context.Context)        {}

func (n *alterRoleOrUserNode) FastPathResults() (int, bool) {
	return n.run.rowsAffected, true
}
