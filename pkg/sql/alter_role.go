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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/authentication"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

// AlterRoleNode represents an ALTER ROLE ... [WITH] OPTION... statement.
type alterRoleNode struct {
	userNameInfo
	ifExists    bool
	isRole      bool
	roleOptions roleoption.List
}

// AlterRole represents a ALTER ROLE statement.
// Privileges: CREATEROLE privilege.
func (p *planner) AlterRole(ctx context.Context, n *tree.AlterRole) (planNode, error) {
	return p.AlterRoleNode(ctx, n.Name, n.IfExists, n.IsRole, "ALTER ROLE", n.KVOptions)
}

func (p *planner) AlterRoleNode(
	ctx context.Context,
	nameE tree.Expr,
	ifExists bool,
	isRole bool,
	opName string,
	kvOptions tree.KVOptions,
) (*alterRoleNode, error) {
	// Note that for Postgres, only superuser can ALTER another superuser.
	// CockroachDB does not support superuser privilege right now.
	// However we make it so the admin role cannot be edited (done in startExec).
	if err := p.CheckRoleOption(ctx, roleoption.CREATEROLE); err != nil {
		return nil, err
	}

	asStringOrNull := func(e tree.Expr, op string) (func() (bool, string, error), error) {
		return p.TypeAsStringOrNull(ctx, e, op)
	}
	roleOptions, err := kvOptions.ToRoleOptions(asStringOrNull, opName)
	if err != nil {
		return nil, err
	}
	if err := roleOptions.CheckRoleOptionConflicts(); err != nil {
		return nil, err
	}

	// Check that the requested combination of password options is
	// compatible with the user's own CREATELOGIN privilege.
	if err := p.checkPasswordOptionConstraints(ctx, roleOptions, false /* newUser */); err != nil {
		return nil, err
	}

	ua, err := p.getUserAuthInfo(ctx, nameE, opName)
	if err != nil {
		return nil, err
	}

	return &alterRoleNode{
		userNameInfo: ua,
		ifExists:     ifExists,
		isRole:       isRole,
		roleOptions:  roleOptions,
	}, nil
}

func (p *planner) checkPasswordOptionConstraints(
	ctx context.Context, roleOptions roleoption.List, newUser bool,
) error {
	if !p.EvalContext().Settings.Version.IsActive(ctx, clusterversion.CreateLoginPrivilege) {
		// TODO(knz): Remove this condition in 21.1.
		if roleOptions.Contains(roleoption.CREATELOGIN) || roleOptions.Contains(roleoption.NOCREATELOGIN) {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				`granting CREATELOGIN or NOCREATELOGIN requires all nodes to be upgraded to %s`,
				clusterversion.ByKey(clusterversion.CreateLoginPrivilege))
		}
	}

	if roleOptions.Contains(roleoption.CREATELOGIN) ||
		roleOptions.Contains(roleoption.NOCREATELOGIN) ||
		roleOptions.Contains(roleoption.PASSWORD) ||
		roleOptions.Contains(roleoption.VALIDUNTIL) ||
		roleOptions.Contains(roleoption.LOGIN) ||
		// CREATE ROLE NOLOGIN is valid without CREATELOGIN.
		(roleOptions.Contains(roleoption.NOLOGIN) && !newUser) ||
		// Disallow implicit LOGIN upon new user.
		(newUser && !roleOptions.Contains(roleoption.NOLOGIN) && !roleOptions.Contains(roleoption.LOGIN)) {
		// Only a role who has CREATELOGIN itself can grant CREATELOGIN or
		// NOCREATELOGIN to another role, or set up a password for
		// authentication, or set up password validity, or enable/disable
		// LOGIN privilege; even if they have CREATEROLE privilege.
		if err := p.CheckRoleOption(ctx, roleoption.CREATELOGIN); err != nil {
			return err
		}
	}
	return nil
}

func (n *alterRoleNode) startExec(params runParams) error {
	var opName string
	if n.isRole {
		sqltelemetry.IncIAMAlterCounter(sqltelemetry.Role)
		opName = "alter-role"
	} else {
		sqltelemetry.IncIAMAlterCounter(sqltelemetry.User)
		opName = "alter-user"
	}
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
		opName,
		params.p.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		fmt.Sprintf("SELECT 1 FROM %s WHERE username = $1", authentication.UsersTableName),
		normalizedUsername,
	)
	if err != nil {
		return err
	}
	if row == nil {
		if n.ifExists {
			return nil
		}
		return errors.Newf("role/user %s does not exist", normalizedUsername)
	}

	if n.roleOptions.Contains(roleoption.PASSWORD) {
		isNull, password, err := n.roleOptions.GetPassword()
		if err != nil {
			return err
		}
		if !isNull && params.extendedEvalCtx.ExecCfg.RPCContext.Config.Insecure {
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

		var hashedPassword []byte
		if !isNull {
			if hashedPassword, err = params.p.checkPasswordAndGetHash(params.ctx, password); err != nil {
				return err
			}
		}

		if hashedPassword == nil {
			// v20.1 and below crash during authentication if they find a NULL value
			// in system.users.hashedPassword. v20.2 and above handle this correctly,
			// but we need to maintain mixed version compatibility for at least one
			// release.
			// TODO(nvanbenschoten): remove this for v21.1.
			hashedPassword = []byte{}
		}

		// Updating PASSWORD is a special case since PASSWORD lives in system.users
		// while the rest of the role options lives in system.role_options.
		_, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			opName,
			params.p.txn,
			`UPDATE system.users SET "hashedPassword" = $2 WHERE username = $1`,
			normalizedUsername,
			hashedPassword,
		)
		if err != nil {
			return err
		}
		if authentication.CacheEnabled.Get(&params.p.ExecCfg().Settings.SV) {
			// Bump user table versions to force a refresh of AuthInfo cache.
			if err := params.p.bumpUsersTableVersion(params.ctx); err != nil {
				return err
			}
		}
	}

	// Get a map of statements to execute for role options and their values.
	stmts, err := n.roleOptions.GetSQLStmts(sqltelemetry.AlterRole)
	if err != nil {
		return err
	}

	for stmt, value := range stmts {
		qargs := []interface{}{normalizedUsername}

		if value != nil {
			isNull, val, err := value()
			if err != nil {
				return err
			}
			if isNull {
				// If the value of the role option is NULL, ensure that nil is passed
				// into the statement placeholder, since val is string type "NULL"
				// will not be interpreted as NULL by the InternalExecutor.
				qargs = append(qargs, nil)
			} else {
				qargs = append(qargs, val)
			}
		}

		_, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			opName,
			params.p.txn,
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			stmt,
			qargs...,
		)
		if err != nil {
			return err
		}
	}

	optStrs := make([]string, len(n.roleOptions))
	for i := range optStrs {
		optStrs[i] = n.roleOptions[i].String()
	}

	if authentication.CacheEnabled.Get(&params.p.ExecCfg().Settings.SV) {
		// Bump role_options table versions to force a refresh of AuthInfo cache.
		if err := params.p.bumpRoleOptionsTableVersion(params.ctx); err != nil {
			return err
		}
	}

	return params.p.logEvent(params.ctx,
		0, /* no target */
		&eventpb.AlterRole{
			RoleName: normalizedUsername.Normalized(),
			Options:  optStrs,
		})
}

func (*alterRoleNode) Next(runParams) (bool, error) { return false, nil }
func (*alterRoleNode) Values() tree.Datums          { return tree.Datums{} }
func (*alterRoleNode) Close(context.Context)        {}
