// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/password"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// CreateRoleNode creates entries in the system.users table.
// This is called from CREATE USER and CREATE ROLE.
type CreateRoleNode struct {
	zeroInputPlanNode
	ifNotExists bool
	isRole      bool
	roleOptions roleoption.List
	roleName    username.SQLUsername
}

// CreateRole represents a CREATE ROLE statement.
// Privileges: INSERT on system.users.
//
//	notes: postgres allows the creation of users with an empty password. We do
//	       as well, but disallow password authentication for these users.
func (p *planner) CreateRole(ctx context.Context, n *tree.CreateRole) (planNode, error) {
	return p.CreateRoleNode(ctx, n.Name, n.IfNotExists, n.IsRole,
		"CREATE ROLE", n.KVOptions)
}

// CreateRoleNode creates a "create user" plan node.
// This can be called from CREATE USER or CREATE ROLE.
func (p *planner) CreateRoleNode(
	ctx context.Context,
	roleSpec tree.RoleSpec,
	ifNotExists bool,
	isRole bool,
	opName string,
	kvOptions tree.KVOptions,
) (*CreateRoleNode, error) {
	if err := p.CheckGlobalPrivilegeOrRoleOption(ctx, privilege.CREATEROLE); err != nil {
		return nil, err
	}

	if roleSpec.RoleSpecType != tree.RoleName {
		return nil, pgerror.Newf(pgcode.ReservedName, "%s cannot be used as a role name here", roleSpec.RoleSpecType)
	}

	roleOptions, err := roleoption.MakeListFromKVOptions(
		ctx, kvOptions, p.ExprEvaluator(opName).LazyStringOrNull,
	)
	if err != nil {
		return nil, err
	}

	if roleOptions.Contains(roleoption.CONTROLCHANGEFEED) {
		p.BufferClientNotice(ctx, pgnotice.Newf(roleoption.ControlChangefeedDeprecationNoticeMsg))
	}

	if err := roleOptions.CheckRoleOptionConflicts(); err != nil {
		return nil, err
	}

	// Using CREATE ROLE syntax enables NOLOGIN by default.
	if isRole && !roleOptions.Contains(roleoption.LOGIN) && !roleOptions.Contains(roleoption.NOLOGIN) {
		roleOptions = append(roleOptions,
			roleoption.RoleOption{Option: roleoption.NOLOGIN, HasValue: false})
	}

	// Check that the requested combination of password options is
	// compatible with the user's own CREATELOGIN privilege.
	if err := p.checkPasswordOptionConstraints(ctx, roleOptions, true /* newUser */); err != nil {
		return nil, err
	}

	roleName, err := decodeusername.FromRoleSpec(
		p.SessionData(), username.PurposeCreation, roleSpec,
	)
	if err != nil {
		return nil, err
	}
	// Reject the reserved roles.
	if roleName.IsReserved() {
		return nil, pgerror.Newf(
			pgcode.ReservedName,
			"role name %q is reserved",
			roleName.Normalized(),
		)
	}

	return &CreateRoleNode{
		roleName:    roleName,
		ifNotExists: ifNotExists,
		isRole:      isRole,
		roleOptions: roleOptions,
	}, nil
}

func (n *CreateRoleNode) startExec(params runParams) error {
	var opName redact.RedactableString
	if n.isRole {
		sqltelemetry.IncIAMCreateCounter(sqltelemetry.Role)
		opName = "create-role"
	} else {
		sqltelemetry.IncIAMCreateCounter(sqltelemetry.User)
		opName = "create-user"
	}

	_, hashedPassword, err := retrievePasswordFromRoleOptions(params, n.roleOptions)
	if err != nil {
		return err
	}

	// Check if the user/role exists.
	row, err := params.p.InternalSQLTxn().QueryRowEx(
		params.ctx,
		opName,
		params.p.txn,
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(`select "isRole" from system.public.%s where username = $1`, catconstants.UsersTableName),
		n.roleName,
	)
	if err != nil {
		return errors.Wrapf(err, "error looking up user")
	}
	if row != nil {
		if n.ifNotExists {
			return nil
		}
		return pgerror.Newf(pgcode.DuplicateObject,
			"a role/user named %s already exists", n.roleName.Normalized())
	}

	// TODO(richardjcai): move hashedPassword column to system.role_options.
	stmt := fmt.Sprintf("INSERT INTO system.public.%s VALUES ($1, $2, $3, $4)", catconstants.UsersTableName)
	roleID, err := descidgen.GenerateUniqueRoleID(params.ctx, params.ExecCfg().DB, params.ExecCfg().Codec)
	if err != nil {
		return err
	}
	rowsAffected, err := params.p.InternalSQLTxn().ExecEx(
		params.ctx, opName, params.p.txn,
		sessiondata.NodeUserSessionDataOverride,
		stmt, n.roleName, hashedPassword, n.isRole, roleID,
	)
	if err != nil {
		return err
	} else if rowsAffected != 1 {
		return errors.AssertionFailedf("%d rows affected by user creation; expected exactly one row affected",
			rowsAffected,
		)
	}

	_, err = updateRoleOptions(params, opName, n.roleOptions, n.roleName, sqltelemetry.CreateRole)
	if err != nil {
		return err
	}

	if sessioninit.CacheEnabled.Get(&params.p.ExecCfg().Settings.SV) {
		// Bump role-related table versions to force a refresh of AuthInfo cache.
		if err := params.p.bumpUsersTableVersion(params.ctx); err != nil {
			return err
		}
		if err := params.p.bumpRoleOptionsTableVersion(params.ctx); err != nil {
			return err
		}
	}
	// Bump role membership table version to force a refresh of role membership
	// cache.
	if err := params.p.BumpRoleMembershipTableVersion(params.ctx); err != nil {
		return err
	}

	return params.p.logEvent(params.ctx,
		0, /* no target */
		&eventpb.CreateRole{RoleName: n.roleName.Normalized()})
}

func updateRoleOptions(
	params runParams,
	opName redact.RedactableString,
	roleOptions roleoption.List,
	roleName username.SQLUsername,
	telemetryOp string,
) (rowsAffected int, err error) {
	// Get a map of statements to execute for role options and their values.
	stmts, err := roleOptions.GetSQLStmts(func(o roleoption.Option) {
		sqltelemetry.IncIAMOptionCounter(telemetryOp, strings.ToLower(o.String()))
	})
	if err != nil {
		return 0, err
	}

	rowsAffected = 0
	for stmt, v := range stmts {
		qargs := []interface{}{roleName}

		if v.Value != nil {
			isNull, val, err := v.Value()
			if err != nil {
				return 0, err
			}
			if isNull {
				// If the value of the role option is NULL, ensure that nil is passed
				// into the statement placeholder, since val is string type "NULL"
				// will not be interpreted as NULL by the Executor.
				qargs = append(qargs, nil)
			} else {
				if v.Validate != nil {
					if err := v.Validate(params.ExecCfg().Settings, roleName, val); err != nil {
						return 0, err
					}
				}
				qargs = append(qargs, val)
			}
		}

		idRow, err := params.p.InternalSQLTxn().QueryRowEx(
			params.ctx, `get-user-id`, params.p.Txn(), sessiondata.NodeUserSessionDataOverride,
			`SELECT user_id FROM system.users WHERE username = $1`, roleName.Normalized(),
		)
		if err != nil {
			return 0, err
		}
		qargs = append(qargs, tree.MustBeDOid(idRow[0]))

		affected, err := params.p.InternalSQLTxn().ExecEx(
			params.ctx,
			opName,
			params.p.txn,
			sessiondata.NodeUserSessionDataOverride,
			stmt,
			qargs...,
		)
		if err != nil {
			return 0, err
		}

		rowsAffected += affected
	}

	return rowsAffected, err
}

// Next implements the planNode interface.
func (*CreateRoleNode) Next(runParams) (bool, error) { return false, nil }

// Values implements the planNode interface.
func (*CreateRoleNode) Values() tree.Datums { return tree.Datums{} }

// Close implements the planNode interface.
func (*CreateRoleNode) Close(context.Context) {}

func retrievePasswordFromRoleOptions(
	params runParams, roleOptions roleoption.List,
) (hasPasswordOpt bool, hashedPassword []byte, err error) {
	if !roleOptions.Contains(roleoption.PASSWORD) {
		return false, nil, nil
	}
	isNull, password, err := roleOptions.GetPassword()
	if err != nil {
		return true, nil, err
	}
	if !isNull && params.extendedEvalCtx.ExecCfg.RPCContext.Insecure {
		// We disallow setting a non-empty password in insecure mode
		// because insecure means an observer may have MITM'ed the change
		// and learned the password.
		//
		// It's valid to clear the password (WITH PASSWORD NULL) however
		// since that forces cert auth when moving back to secure mode,
		// and certs can't be MITM'ed over the insecure SQL connection.
		return true, nil, pgerror.New(pgcode.InvalidPassword,
			"setting or updating a password is not supported in insecure mode")
	}

	if !isNull {
		if hashedPassword, err = params.p.checkPasswordAndGetHash(params.ctx, password); err != nil {
			return true, nil, err
		}
	}

	return true, hashedPassword, nil
}

func (p *planner) checkPasswordAndGetHash(
	ctx context.Context, passwordStr string,
) (hashedPassword []byte, err error) {
	if passwordStr == "" {
		return hashedPassword, security.ErrEmptyPassword
	}

	st := p.ExecCfg().Settings
	if security.AutoDetectPasswordHashes.Get(&st.SV) {
		var isPreHashed, schemeSupported bool
		var schemeName string
		var issueNum int
		isPreHashed, schemeSupported, issueNum, schemeName, hashedPassword, err = password.CheckPasswordHashValidity([]byte(passwordStr))
		if err != nil {
			return hashedPassword, pgerror.WithCandidateCode(err, pgcode.Syntax)
		}
		if isPreHashed {
			if !schemeSupported {
				return hashedPassword, unimplemented.NewWithIssueDetailf(issueNum, schemeName, "the password hash scheme %q is not supported", schemeName)
			}
			return hashedPassword, nil
		}
	}

	if minLength := security.MinPasswordLength.Get(&st.SV); minLength >= 1 && int64(len(passwordStr)) < minLength {
		return nil, errors.WithHintf(security.ErrPasswordTooShort,
			"Passwords must be %d characters or longer.", minLength)
	}

	method := security.GetConfiguredPasswordHashMethod(&st.SV)
	cost, err := security.GetConfiguredPasswordCost(ctx, &st.SV, method)
	if err != nil {
		return hashedPassword, errors.HandleAsAssertionFailure(err)
	}
	hashedPassword, err = password.HashPassword(ctx, cost, method, passwordStr,
		security.GetExpensiveHashComputeSem(ctx))
	if err != nil {
		return hashedPassword, err
	}

	return hashedPassword, nil
}
