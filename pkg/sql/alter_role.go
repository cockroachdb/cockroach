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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

// alterRoleNode represents an ALTER ROLE ... [WITH] OPTION... statement.
type alterRoleNode struct {
	userNameInfo
	ifExists    bool
	isRole      bool
	roleOptions roleoption.List
}

// alterRoleSetNode represents an `ALTER ROLE ... SET` statement.
type alterRoleSetNode struct {
	userNameInfo userNameInfo
	ifExists     bool
	isRole       bool
	allRoles     bool
	// dbDescID == 0 means all databases.
	dbDescID    descpb.ID
	setVarKind  setVarBehavior
	varName     string
	sVar        sessionVar
	typedValues []tree.TypedExpr
}

// setVarBehavior is an enum that describes how to alter the session variable
// defaults when executing alterRoleSetNode.
type setVarBehavior int

const (
	setSingleVar   setVarBehavior = 0
	resetSingleVar setVarBehavior = 1
	resetAllVars   setVarBehavior = 2
	unknown        setVarBehavior = 3
)

// AlterRole represents a `ALTER ROLE ... [WITH] OPTION` statement.
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
	// CockroachDB does not support the superuser role option right now, but we
	// make it so any member of the ADMIN role can only be edited by another ADMIN
	// (done in startExec).
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
		fmt.Sprintf("SELECT 1 FROM %s WHERE username = $1", sessioninit.UsersTableName),
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

	isAdmin, err := params.p.UserHasAdminRole(params.ctx, normalizedUsername)
	if err != nil {
		return err
	}
	if isAdmin {
		if err := params.p.RequireAdminRole(params.ctx, "ALTER ROLE admin"); err != nil {
			return err
		}
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
		if sessioninit.CacheEnabled.Get(&params.p.ExecCfg().Settings.SV) {
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

	if sessioninit.CacheEnabled.Get(&params.p.ExecCfg().Settings.SV) {
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

// AlterRoleSet represents a `ALTER ROLE ... SET` statement.
// Privileges: CREATEROLE privilege; or admin-only if `ALTER ROLE ALL`.
func (p *planner) AlterRoleSet(ctx context.Context, n *tree.AlterRoleSet) (planNode, error) {
	// Note that for Postgres, only superuser can ALTER another superuser.
	// CockroachDB does not support the superuser role option right now.
	// However we make it so members of the ADMIN role can only be edited
	// by other ADMINs (done in startExec).
	// Also note that we diverge from Postgres by prohibiting users from
	// modifying their own defaults unless they have CREATEROLE. This is analogous
	// to our restriction that prevents a user from modifying their own password.
	if n.AllRoles {
		if err := p.RequireAdminRole(ctx, "ALTER ROLE ALL"); err != nil {
			return nil, err
		}
	} else {
		if err := p.CheckRoleOption(ctx, roleoption.CREATEROLE); err != nil {
			return nil, err
		}
	}

	// TODO(rafi): Remove this condition in 21.2.
	if !p.EvalContext().Settings.Version.IsActive(ctx, clusterversion.DatabaseRoleSettings) {
		return nil, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			`altering per-role defaults requires all nodes to be upgraded to %s`,
			clusterversion.ByKey(clusterversion.DatabaseRoleSettings))
	}

	var ua userNameInfo
	if !n.AllRoles {
		var err error
		ua, err = p.getUserAuthInfo(ctx, n.RoleName, "ALTER ROLE")
		if err != nil {
			return nil, err
		}
	}

	dbDescID := descpb.ID(0)
	if n.DatabaseName != "" {
		dbDesc, err := p.Descriptors().GetImmutableDatabaseByName(ctx, p.txn, string(n.DatabaseName),
			tree.DatabaseLookupFlags{Required: true})
		if err != nil {
			return nil, err
		}
		dbDescID = dbDesc.GetID()
	}

	setVarKind, varName, sVar, typedValues, err := p.processSetOrResetClause(ctx, n.SetOrReset)
	if err != nil {
		return nil, err
	}

	return &alterRoleSetNode{
		userNameInfo: ua,
		ifExists:     n.IfExists,
		isRole:       n.IsRole,
		allRoles:     n.AllRoles,
		dbDescID:     dbDescID,
		setVarKind:   setVarKind,
		varName:      varName,
		sVar:         sVar,
		typedValues:  typedValues,
	}, nil
}

func (p *planner) processSetOrResetClause(
	ctx context.Context, setOrResetClause *tree.SetVar,
) (
	setVarKind setVarBehavior,
	varName string,
	sVar sessionVar,
	typedValues []tree.TypedExpr,
	err error,
) {
	if setOrResetClause.ResetAll {
		return resetAllVars, "", sessionVar{}, nil, nil
	}

	if setOrResetClause.Name == "" {
		// The user entered `SET "" = foo`. Reject it.
		return unknown, "", sessionVar{}, nil, pgerror.Newf(pgcode.Syntax, "invalid variable name: %q", setOrResetClause.Name)
	}

	isReset := false
	if len(setOrResetClause.Values) == 1 {
		if _, ok := setOrResetClause.Values[0].(tree.DefaultVal); ok {
			// `SET var = DEFAULT` means RESET.
			// In that case, we want typedValues to remain nil.
			isReset = true
		}
	}
	varName = strings.ToLower(setOrResetClause.Name)

	// For RESET, we shouldn't do any validation on the varName at all.
	if isReset {
		return resetSingleVar, varName, sessionVar{}, nil, nil
	}

	switch varName {
	// The "database" setting can't be configured here, since the
	// default settings are stored per-database.
	// The "role" setting can't be configured here, since we are already
	// that role.
	case "database", "role":
		return unknown, "", sessionVar{}, nil, newCannotChangeParameterError(varName)
	}
	_, sVar, err = getSessionVar(varName, false /* missingOk */)
	if err != nil {
		return unknown, "", sessionVar{}, nil, err
	}

	// There must be a `Set` function defined. `RuntimeSet` is not allowed here
	// since `RuntimeSet` cannot be used during session initialization.
	if sVar.Set == nil {
		return unknown, "", sessionVar{}, nil, newCannotChangeParameterError(varName)
	}

	// The typedValues will be turned into a string in startExec.
	for _, expr := range setOrResetClause.Values {
		expr = paramparse.UnresolvedNameToStrVal(expr)

		typedValue, err := p.analyzeExpr(
			ctx, expr, nil, tree.IndexedVarHelper{}, types.String, false, "ALTER ROLE ... SET ",
		)
		if err != nil {
			return unknown, "", sessionVar{}, nil, wrapSetVarError(varName, expr.String(), "%v", err)
		}
		typedValues = append(typedValues, typedValue)
	}

	return setSingleVar, varName, sVar, typedValues, nil
}

func (n *alterRoleSetNode) startExec(params runParams) error {
	var opName string
	if n.isRole {
		sqltelemetry.IncIAMAlterCounter(sqltelemetry.Role)
		opName = "alter-role"
	} else {
		sqltelemetry.IncIAMAlterCounter(sqltelemetry.User)
		opName = "alter-user"
	}

	needsUpdate, roleName, err := n.getRoleName(params, opName)
	if err != nil {
		return err
	}
	if !needsUpdate {
		// Nothing to do if called with `IF EXISTS` for a role that doesn't exist.
		return nil
	}

	var deleteQuery = fmt.Sprintf(
		`DELETE FROM %s WHERE database_id = $1 AND role_name = $2`,
		sessioninit.DatabaseRoleSettingsTableName,
	)
	var upsertQuery = fmt.Sprintf(
		`UPSERT INTO %s (database_id, role_name, settings) VALUES ($1, $2, $3)`,
		sessioninit.DatabaseRoleSettingsTableName,
	)

	// Instead of inserting an empty settings array, this function will make
	// sure the row is deleted instead.
	upsertOrDeleteFunc := func(newSettings []string) error {
		var rowsAffected int
		var internalExecErr error
		if newSettings == nil {
			rowsAffected, internalExecErr = params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
				params.ctx,
				opName,
				params.p.txn,
				sessiondata.InternalExecutorOverride{User: security.RootUserName()},
				deleteQuery,
				n.dbDescID,
				roleName,
			)
		} else {
			rowsAffected, internalExecErr = params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
				params.ctx,
				opName,
				params.p.txn,
				sessiondata.InternalExecutorOverride{User: security.RootUserName()},
				upsertQuery,
				n.dbDescID,
				roleName,
				newSettings,
			)
		}
		if internalExecErr != nil {
			return internalExecErr
		}

		if rowsAffected > 0 && sessioninit.CacheEnabled.Get(&params.p.ExecCfg().Settings.SV) {
			// Bump database_role_settings table versions to force a refresh of AuthInfo cache.
			if err := params.p.bumpDatabaseRoleSettingsTableVersion(params.ctx); err != nil {
				return err
			}
		}
		return params.p.logEvent(params.ctx,
			0, /* no target */
			&eventpb.AlterRole{
				RoleName: roleName.Normalized(),
				Options:  []string{roleoption.DEFAULTSETTINGS.String()},
			})
	}

	if n.setVarKind == resetAllVars {
		return upsertOrDeleteFunc(nil)
	}

	hasOldSettings, newSettings, err := n.makeNewSettings(params, opName, roleName)
	if err != nil {
		return err
	}

	if n.setVarKind == resetSingleVar {
		if !hasOldSettings {
			return nil
		}
		return upsertOrDeleteFunc(newSettings)
	}

	// The remaining case is `SET var = val`, to add a default setting.
	strVal, err := n.getSessionVarVal(params)
	if err != nil {
		return err
	}

	newSetting := fmt.Sprintf("%s=%s", n.varName, strVal)
	newSettings = append(newSettings, newSetting)
	return upsertOrDeleteFunc(newSettings)
}

// getRoleName resolves the roleName and performs additional validation
// to make sure the role is safe to edit.
func (n *alterRoleSetNode) getRoleName(
	params runParams, opName string,
) (needsUpdate bool, roleName security.SQLUsername, err error) {
	if n.allRoles {
		return true, security.MakeSQLUsernameFromPreNormalizedString(""), nil
	}
	name, err := n.userNameInfo.name()
	if err != nil {
		return false, security.SQLUsername{}, err
	}
	if name == "" {
		return false, security.SQLUsername{}, errNoUserNameSpecified
	}
	roleName, err = NormalizeAndValidateUsername(name)
	if err != nil {
		return false, security.SQLUsername{}, err
	}
	if roleName.IsAdminRole() {
		return false, security.SQLUsername{}, pgerror.Newf(pgcode.InsufficientPrivilege, "cannot edit admin role")
	}
	if roleName.IsRootUser() {
		return false, security.SQLUsername{}, pgerror.Newf(pgcode.InsufficientPrivilege, "cannot edit root user")
	}
	if roleName.IsPublicRole() {
		return false, security.SQLUsername{}, pgerror.Newf(pgcode.InsufficientPrivilege, "cannot edit public role")
	}
	// Check if role exists.
	row, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.QueryRowEx(
		params.ctx,
		opName,
		params.p.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		fmt.Sprintf("SELECT 1 FROM %s WHERE username = $1", sessioninit.UsersTableName),
		roleName,
	)
	if err != nil {
		return false, security.SQLUsername{}, err
	}
	if row == nil {
		if n.ifExists {
			return false, security.SQLUsername{}, nil
		}
		return false, security.SQLUsername{}, errors.Newf("role/user %s does not exist", roleName)
	}
	isAdmin, err := params.p.UserHasAdminRole(params.ctx, roleName)
	if err != nil {
		return false, security.SQLUsername{}, err
	}
	if isAdmin {
		if err := params.p.RequireAdminRole(params.ctx, "ALTER ROLE admin"); err != nil {
			return false, security.SQLUsername{}, err
		}
	}
	return true, roleName, nil
}

// makeNewSettings first loads the existing settings for the (role, db), then
// returns a newSettings list with any occurrence of varName removed.
func (n *alterRoleSetNode) makeNewSettings(
	params runParams, opName string, roleName security.SQLUsername,
) (hasOldSettings bool, newSettings []string, err error) {
	var selectQuery = fmt.Sprintf(
		`SELECT settings FROM %s WHERE database_id = $1 AND role_name = $2`,
		sessioninit.DatabaseRoleSettingsTableName,
	)
	datums, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.QueryRowEx(
		params.ctx,
		opName,
		params.p.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		selectQuery,
		n.dbDescID,
		roleName,
	)
	if err != nil {
		return false, nil, err
	}
	var oldSettings *tree.DArray
	if datums != nil {
		oldSettings = tree.MustBeDArray(datums[0])
		for _, s := range oldSettings.Array {
			oldSetting := string(tree.MustBeDString(s))
			keyVal := strings.SplitN(oldSetting, "=", 2)
			if !strings.EqualFold(n.varName, keyVal[0]) {
				newSettings = append(newSettings, oldSetting)
			}
		}
	}
	return oldSettings != nil, newSettings, nil
}

// getSessionVarVal evaluates typedValues to get a string value that can
// be persisted as the default setting for the session variable. It also
// performs validation to make sure the session variable exists and is
// configurable with the given value.
func (n *alterRoleSetNode) getSessionVarVal(params runParams) (string, error) {
	if n.varName == "" || n.typedValues == nil {
		return "", nil
	}
	for i, v := range n.typedValues {
		d, err := v.Eval(params.EvalContext())
		if err != nil {
			return "", err
		}
		n.typedValues[i] = d
	}
	var strVal string
	var err error
	if n.sVar.GetStringVal != nil {
		strVal, err = n.sVar.GetStringVal(params.ctx, params.extendedEvalCtx, n.typedValues)
	} else {
		// No string converter defined, use the default one.
		strVal, err = getStringVal(params.EvalContext(), n.varName, n.typedValues)
	}
	if err != nil {
		return "", err
	}

	// Validate the new string value, but don't actually apply it to any real
	// session.
	if err := CheckSessionVariableValueValid(params.ctx, params.ExecCfg().Settings, n.varName, strVal); err != nil {
		return "", err
	}
	return strVal, nil
}

func (*alterRoleSetNode) Next(runParams) (bool, error) { return false, nil }
func (*alterRoleSetNode) Values() tree.Datums          { return tree.Datums{} }
func (*alterRoleSetNode) Close(context.Context)        {}
