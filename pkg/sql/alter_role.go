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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/roleprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// alterRoleNode represents an ALTER ROLE ... WITH [OPTIONS...] statement.
type alterRoleNode struct {
	name           func() (string, error)
	ifExists       bool
	rolePrivileges roleprivilege.List

	run alterRoleRun
}

// AlterRolePrivileges alters a user's permissios
func (p *planner) AlterRolePrivileges(
	ctx context.Context, n *tree.AlterRolePrivileges,
) (*alterRoleNode, error) {
	// Note that for Postgres, only superuser can ALTER another superuser
	// CockroachDB does not support superuser privilege right now
	// However we make it so the admin role cannot be edited (done in startExec)
	if err := p.HasCreateRolePrivilege(ctx); err != nil {
		return nil, err
	}

	rolePrivilegeBits, err := n.RolePrivileges.ToBitField()
	if err != nil {
		return nil, err
	}

	if err := roleprivilege.CheckRolePrivilegeConflicts(rolePrivilegeBits); err != nil {
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
// alterRoleRun	 for local execution.
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
		return errors.New("cannot edit admin role")
	}
	normalizedUsername, err := NormalizeAndValidateUsername(name)
	if err != nil {
		return err
	}

	rolePrivilegeBits, err := n.rolePrivileges.ToBitField()
	if err != nil {
		return err
	}

	n.run.rowsAffected, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
		params.ctx,
		"update-role",
		params.p.txn,
		`UPDATE system.users SET "hasCreateRole" = $2 WHERE username = $1`,
		normalizedUsername,
		rolePrivilegeBits&roleprivilege.CREATEROLE.Mask() != 0,
	)
	if err != nil {
		return err
	}
	if n.run.rowsAffected == 0 && !n.ifExists {
		return pgerror.Newf(pgcode.UndefinedObject,
			"user %s does not exist", name)
	}
	return err
}

func (*alterRoleNode) Next(runParams) (bool, error) { return false, nil }
func (*alterRoleNode) Values() tree.Datums          { return tree.Datums{} }
func (*alterRoleNode) Close(context.Context)        {}

func (n *alterRoleNode) FastPathResults() (int, bool) {
	return n.run.rowsAffected, true
}
