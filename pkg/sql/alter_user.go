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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
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
	tDesc, err := getTableDesc(ctx, p.txn, p.getVirtualTabler(), userTableName)
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
		return errors.Errorf("user %s cannot use password authentication", security.RootUser)
	}

	if len(hashedPassword) > 0 && params.extendedEvalCtx.ExecCfg.RPCContext.Insecure {
		return errors.New("cluster in insecure mode; user cannot use password authentication")
	}

	internalExecutor := InternalExecutor{ExecCfg: params.extendedEvalCtx.ExecCfg}
	n.run.rowsAffected, err = internalExecutor.ExecuteStatementInTransaction(
		params.ctx,
		"create-user",
		params.p.txn,
		`UPDATE system.public.users SET "hashedPassword" = $2 WHERE username = $1 AND "isRole" = false`,
		normalizedUsername,
		hashedPassword,
	)
	if err != nil {
		return err
	}
	if n.run.rowsAffected == 0 && !n.ifExists {
		return errors.Errorf("user %s does not exist", normalizedUsername)
	}
	return err
}

func (*alterUserSetPasswordNode) Next(runParams) (bool, error) { return false, nil }
func (*alterUserSetPasswordNode) Values() tree.Datums          { return tree.Datums{} }
func (*alterUserSetPasswordNode) Close(context.Context)        {}

func (n *alterUserSetPasswordNode) FastPathResults() (int, bool) {
	return n.run.rowsAffected, true
}
