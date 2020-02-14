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
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// alterRoleNode represents an ALTER ROLE ... [WITH] OPTION... statement.
type alterRoleNode struct {
	userNameInfo
	roleOptions roleoption.List

	run alterRoleRun
}

// AlterRoleOptions alters a user's permissios
func (p *planner) AlterRoleOptions(
	ctx context.Context, n *tree.AlterRoleOptions,
) (*alterRoleNode, error) {
	// Note that for Postgres, only superuser can ALTER another superuser.
	// CockroachDB does not support superuser privilege right now.
	// However we make it so the admin role cannot be edited (done in startExec).
	if err := p.HasRoleOption(ctx, roleoption.CREATEROLE); err != nil {
		return nil, err
	}

	roleOptions, err := n.OptionsWithValues.ToRoleOptions(p.TypeAsString, "ALTER ROLE")
	if err != nil {
		return nil, err
	}
	if err := roleOptions.CheckRoleOptionConflicts(); err != nil {
		return nil, err
	}

	ua, err := p.getUserAuthInfo(n.Name, "ALTER ROLE")
	if err != nil {
		return nil, err
	}

	return &alterRoleNode{
		userNameInfo: ua,
		roleOptions:  roleOptions,
	}, nil
}

// alterRoleRun is the run-time state of
// alterRoleNode for local execution.
type alterRoleRun struct {
	rowsAffected int
}

func (n *alterRoleNode) startExec(params runParams) error {
	normalizedUsername, err := n.userNameInfo.resolveUsername()
	if err != nil {
		return err
	}

	if normalizedUsername == "admin" {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"cannot edit admin role")
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
		return errors.Newf("role/user %s does not exist", normalizedUsername)
	}

	if n.roleOptions.Contains(roleoption.PASSWORD) {
		hashedPassword, err := n.roleOptions.GetHashedPassword()
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
	}

	stmts, err := n.roleOptions.GetSQLStmts()
	if err != nil {
		return err
	}

	for _, stmt := range stmts {
		rowsAffected, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
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
		n.run.rowsAffected += rowsAffected
	}
	return nil
}

func (*alterRoleNode) Next(runParams) (bool, error) { return false, nil }
func (*alterRoleNode) Values() tree.Datums          { return tree.Datums{} }
func (*alterRoleNode) Close(context.Context)        {}

func (n *alterRoleNode) FastPathResults() (int, bool) {
	return n.run.rowsAffected, true
}
