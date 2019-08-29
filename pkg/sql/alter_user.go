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
)

// alterUserSetPasswordNode represents an ALTER USER ... WITH PASSWORD statement.
type alterUserSetPasswordNode struct {
	userAuthInfo
	ifExists bool

	run alterUserSetPasswordRun
}

// AlterUserSetPassword changes a user's password.
// Privileges: UPDATE on the users table.
func (p *planner) AlterUserSetPassword(
	ctx context.Context, n *tree.AlterUserSetPassword,
) (planNode, error) {
	tDesc, err := ResolveExistingObject(ctx, p, userTableName, tree.ObjectLookupFlagsWithRequired(), ResolveRequireTableDesc)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, tDesc, privilege.UPDATE); err != nil {
		return nil, err
	}

	ua, err := p.getUserAuthInfo(n.Name, n.Password, "ALTER USER")
	if err != nil {
		return nil, err
	}

	return &alterUserSetPasswordNode{
		userAuthInfo: ua,
		ifExists:     n.IfExists,
	}, nil
}

// alterUserSetPasswordRun is the run-time state of
// alterUserSetPasswordNode for local execution.
type alterUserSetPasswordRun struct {
	rowsAffected int
}

func (n *alterUserSetPasswordNode) startExec(params runParams) error {
	normalizedUsername, hashedPassword, err := n.userAuthInfo.resolve()
	if err != nil {
		return err
	}

	// The root user is not allowed a password.
	if normalizedUsername == security.RootUser {
		return pgerror.Newf(pgcode.InvalidPassword,
			"user %s cannot use password authentication", security.RootUser)
	}

	if len(hashedPassword) > 0 && params.extendedEvalCtx.ExecCfg.RPCContext.Insecure {
		return pgerror.New(pgcode.InvalidPassword,
			"cluster in insecure mode; user cannot use password authentication")
	}

	n.run.rowsAffected, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
		params.ctx,
		"update-user",
		params.p.txn,
		`UPDATE system.users SET "hashedPassword" = $2 WHERE username = $1 AND "isRole" = false`,
		normalizedUsername,
		hashedPassword,
	)
	if err != nil {
		return err
	}
	if n.run.rowsAffected == 0 && !n.ifExists {
		return pgerror.Newf(pgcode.UndefinedObject,
			"user %s does not exist", normalizedUsername)
	}
	return err
}

func (*alterUserSetPasswordNode) Next(runParams) (bool, error) { return false, nil }
func (*alterUserSetPasswordNode) Values() tree.Datums          { return tree.Datums{} }
func (*alterUserSetPasswordNode) Close(context.Context)        {}

func (n *alterUserSetPasswordNode) FastPathResults() (int, bool) {
	return n.run.rowsAffected, true
}
