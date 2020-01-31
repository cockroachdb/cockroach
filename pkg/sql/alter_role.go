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
	name        func() (string, error)
	roleOptions roleoption.List

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

	if err := n.RoleOptions.CheckRoleOptionConflicts(); err != nil {
		return nil, err
	}

	name, err := p.TypeAsString(n.Name, "ALTER ROLE")
	if err != nil {
		return nil, err
	}

	return &alterRoleOrUserNode{
		name:        name,
		roleOptions: n.RoleOptions,
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
	normalizedUsername, err := NormalizeAndValidateUsername(name)
	if err != nil {
		return err
	}

	setStmt, err := n.roleOptions.CreateSetStmtFromRoleOptions()
	if err != nil {
		return err
	}

	n.run.rowsAffected, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
		params.ctx,
		"update-role",
		params.p.txn,
		fmt.Sprintf(`UPDATE %s %s WHERE username = $1`, userTableName, setStmt),
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
