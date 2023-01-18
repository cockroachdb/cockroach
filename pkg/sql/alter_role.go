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

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

// alterRoleNode represents an ALTER ROLE ... [WITH] OPTION... statement.
type alterRoleNode struct {
	roleName    username.SQLUsername
	ifExists    bool
	isRole      bool
	roleOptions roleoption.List
}

// alterRoleSetNode represents an `ALTER ROLE ... SET` statement.
type alterRoleSetNode struct {
	roleName username.SQLUsername
	ifExists bool
	isRole   bool
	allRoles bool
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

var changeOwnPasswordEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.auth.change_own_password.enabled",
	"controls whether a user is allowed to change their own password, even if they have no other privileges",
	false,
).WithPublic()

// AlterRole represents a `ALTER ROLE ... [WITH] OPTION` statement.
// Privileges: CREATEROLE privilege.
func (p *planner) AlterRole(ctx context.Context, n *tree.AlterRole) (planNode, error) {
	return p.AlterRoleNode(ctx, n.Name, n.IfExists, n.IsRole, "ALTER ROLE", n.KVOptions)
}

func (p *planner) AlterRoleNode(
	ctx context.Context,
	roleSpec tree.RoleSpec,
	ifExists bool,
	isRole bool,
	opName string,
	kvOptions tree.KVOptions,
) (*alterRoleNode, error) {
	roleOptions, err := roleoption.MakeListFromKVOptions(
		ctx, kvOptions, p.ExprEvaluator(opName).LazyStringOrNull,
	)
	if err != nil {
		return nil, err
	}
	if err := roleOptions.CheckRoleOptionConflicts(); err != nil {
		return nil, err
	}

	if roleOptions.Contains(roleoption.CONTROLCHANGEFEED) {
		p.BufferClientNotice(ctx, pgnotice.Newf(roleoption.ControlChangefeedDeprecationNoticeMsg))
	}

	roleName, err := decodeusername.FromRoleSpec(
		p.SessionData(), username.PurposeValidation, roleSpec,
	)
	if err != nil {
		return nil, err
	}

	return &alterRoleNode{
		roleName:    roleName,
		ifExists:    ifExists,
		isRole:      isRole,
		roleOptions: roleOptions,
	}, nil
}

func (p *planner) checkPasswordOptionConstraints(
	ctx context.Context, roleOptions roleoption.List, newUser bool,
) error {
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
	if n.roleName.Undefined() {
		return pgerror.New(pgcode.InvalidParameterValue, "no username specified")
	}
	if n.roleName.IsAdminRole() {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"cannot edit admin role")
	}

	needMoreChecks := true
	if n.roleName == params.p.SessionData().User() && len(n.roleOptions) == 1 && n.roleOptions.Contains(roleoption.
		PASSWORD) {
		// Note: This call to GetPassword must happen during execution, not during
		// planning, since the password might be ea plaecholder argument that needs
		// to be evaluated at execution time.
		isNull, _, err := n.roleOptions.GetPassword()
		if err != nil {
			return err
		}
		// If sql.auth.change_own_password.enabled is set then the user is allowed to
		// alter their own password to a non-null value without needing CREATEROLE or
		// CREATELOGIN.
		if !isNull && changeOwnPasswordEnabled.Get(params.p.execCfg.SV()) {
			needMoreChecks = false
		}
	}
	if needMoreChecks {
		// Note that for Postgres, only superuser can ALTER another superuser.
		// CockroachDB does not support the superuser role option right now, but we
		// make it so any member of the ADMIN role can only be edited by another ADMIN
		// (done after checking for existence of the role).
		if err := params.p.CheckRoleOption(params.ctx, roleoption.CREATEROLE); err != nil {
			return err
		}
		// Check that the requested combination of password options is
		// compatible with the user's own CREATELOGIN privilege.
		if err := params.p.checkPasswordOptionConstraints(params.ctx, n.roleOptions, false /* newUser */); err != nil {
			return err
		}
	}

	// Check if role exists.
	row, err := params.p.InternalSQLTxn().QueryRowEx(
		params.ctx,
		opName,
		params.p.txn,
		sessiondata.RootUserSessionDataOverride,
		fmt.Sprintf("SELECT 1 FROM %s WHERE username = $1", sessioninit.UsersTableName),
		n.roleName,
	)
	if err != nil {
		return err
	}
	if row == nil {
		if n.ifExists {
			return nil
		}
		return sqlerrors.NewUndefinedUserError(n.roleName)
	}

	isAdmin, err := params.p.UserHasAdminRole(params.ctx, n.roleName)
	if err != nil {
		return err
	}
	if isAdmin {
		if err := params.p.RequireAdminRole(params.ctx, "ALTER ROLE admin"); err != nil {
			return err
		}
	}

	hasPasswordOpt, hashedPassword, err := retrievePasswordFromRoleOptions(params, n.roleOptions)
	if err != nil {
		return err
	}
	if hasPasswordOpt {
		// Updating PASSWORD is a special case since PASSWORD lives in system.users
		// while the rest of the role options lives in system.role_options.
		rowAffected, err := params.p.InternalSQLTxn().ExecEx(
			params.ctx,
			opName,
			params.p.txn,
			sessiondata.NodeUserSessionDataOverride,
			`UPDATE system.users SET "hashedPassword" = $2 WHERE username = $1`,
			n.roleName,
			hashedPassword,
		)
		if err != nil {
			return err
		}
		if sessioninit.CacheEnabled.Get(&params.p.ExecCfg().Settings.SV) && rowAffected > 0 {
			// Bump user table versions to force a refresh of AuthInfo cache.
			if err := params.p.bumpUsersTableVersion(params.ctx); err != nil {
				return err
			}
		}
	}

	rowsAffected, err := updateRoleOptions(params, opName, n.roleOptions, n.roleName, sqltelemetry.AlterRole)
	if err != nil {
		return err
	}

	if sessioninit.CacheEnabled.Get(&params.p.ExecCfg().Settings.SV) && rowsAffected > 0 {
		// Bump role_options table versions to force a refresh of AuthInfo cache.
		if err := params.p.bumpRoleOptionsTableVersion(params.ctx); err != nil {
			return err
		}
	}

	optStrs := make([]string, len(n.roleOptions))
	for i := range optStrs {
		optStrs[i] = n.roleOptions[i].String()
	}

	return params.p.logEvent(params.ctx,
		0, /* no target */
		&eventpb.AlterRole{
			RoleName: n.roleName.Normalized(),
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

	var roleName username.SQLUsername
	if !n.AllRoles {
		var err error
		roleName, err = decodeusername.FromRoleSpec(
			p.SessionData(), username.PurposeValidation, n.RoleName,
		)
		if err != nil {
			return nil, err
		}
	}

	dbDescID := descpb.ID(0)
	if n.DatabaseName != "" {
		dbDesc, err := p.Descriptors().ByNameWithLeased(p.txn).Get().Database(ctx, string(n.DatabaseName))
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
		roleName:    roleName,
		ifExists:    n.IfExists,
		isRole:      n.IsRole,
		allRoles:    n.AllRoles,
		dbDescID:    dbDescID,
		setVarKind:  setVarKind,
		varName:     varName,
		sVar:        sVar,
		typedValues: typedValues,
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
	// The "tracing" setting is handled specially in the grammar, so we skip it
	// here since it doesn't make sense to set as a default anyway.
	case "database", "role", "tracing":
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
			return unknown, "", sessionVar{}, nil, wrapSetVarError(err, varName, expr.String())
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
			rowsAffected, internalExecErr = params.p.InternalSQLTxn().ExecEx(
				params.ctx,
				opName,
				params.p.txn,
				sessiondata.RootUserSessionDataOverride,
				deleteQuery,
				n.dbDescID,
				roleName,
			)
		} else {
			rowsAffected, internalExecErr = params.p.InternalSQLTxn().ExecEx(
				params.ctx,
				opName,
				params.p.txn,
				sessiondata.RootUserSessionDataOverride,
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
				SetInfo:  []string{"DEFAULTSETTINGS"},
			})
	}

	if n.setVarKind == resetAllVars {
		return upsertOrDeleteFunc(nil)
	}

	oldSettings, newSettings, err := n.makeNewSettings(params, opName, roleName)
	if err != nil {
		return err
	}

	if n.setVarKind == resetSingleVar {
		if oldSettings == nil || deepEqualIgnoringOrders(oldSettings, newSettings) {
			// If there is no old settings at all, or reset a setting that's not previously set (i.e. old setting is new
			// setting), then we can exist early.
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
	if deepEqualIgnoringOrders(oldSettings, newSettings) {
		// If the resulting new setting for this role is equal to the old settings, then we can exist early.
		return nil
	}
	return upsertOrDeleteFunc(newSettings)
}

// deepEqualIgnoringOrders returns true if slice1 and slice2 contain exactly the same strings ignoring their ordering.
// E.g. slice1 = ["a", "b", "b"], slice2 = ["b", "a", "b"], return = true
func deepEqualIgnoringOrders(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}

	ss := make(map[string]int)
	add := func(str string, cnt int) {
		if ss[str] += cnt; ss[str] == 0 {
			delete(ss, str)
		}
	}
	for _, s := range s1 {
		add(s, 1)
	}
	for _, s := range s2 {
		add(s, -1)
	}
	return len(ss) == 0
}

// getRoleName resolves the roleName and performs additional validation
// to make sure the role is safe to edit.
func (n *alterRoleSetNode) getRoleName(
	params runParams, opName string,
) (needsUpdate bool, retRoleName username.SQLUsername, err error) {
	if n.allRoles {
		return true, username.MakeSQLUsernameFromPreNormalizedString(""), nil
	}
	if n.roleName.Undefined() {
		return false, username.SQLUsername{}, pgerror.New(pgcode.InvalidParameterValue, "no username specified")
	}
	if n.roleName.IsAdminRole() {
		return false, username.SQLUsername{}, pgerror.Newf(pgcode.InsufficientPrivilege, "cannot edit admin role")
	}
	if n.roleName.IsRootUser() {
		return false, username.SQLUsername{}, pgerror.Newf(pgcode.InsufficientPrivilege, "cannot edit root user")
	}
	if n.roleName.IsPublicRole() {
		return false, username.SQLUsername{}, pgerror.Newf(pgcode.InsufficientPrivilege, "cannot edit public role")
	}
	// Check if role exists.
	row, err := params.p.InternalSQLTxn().QueryRowEx(
		params.ctx,
		opName,
		params.p.txn,
		sessiondata.RootUserSessionDataOverride,
		fmt.Sprintf("SELECT 1 FROM %s WHERE username = $1", sessioninit.UsersTableName),
		n.roleName,
	)
	if err != nil {
		return false, username.SQLUsername{}, err
	}
	if row == nil {
		if n.ifExists {
			return false, username.SQLUsername{}, nil
		}
		return false, username.SQLUsername{}, sqlerrors.NewUndefinedUserError(n.roleName)
	}
	isAdmin, err := params.p.UserHasAdminRole(params.ctx, n.roleName)
	if err != nil {
		return false, username.SQLUsername{}, err
	}
	if isAdmin {
		if err := params.p.RequireAdminRole(params.ctx, "ALTER ROLE admin"); err != nil {
			return false, username.SQLUsername{}, err
		}
	}
	return true, n.roleName, nil
}

// makeNewSettings first loads the existing settings for the (role, db), then
// returns a newSettings list with any occurrence of varName removed.
//
// E.g. Suppose there is an existing row in `system.database_role_settings`:
//
//	(24, max, {timezone=America/New_York, use_declarative_schema_changer=off, statement_timeout=10s})
//
// and
//
//	n.varName = 'use_declarative_schema_changer',
//
// then the return of this function will be
//  1. oldSettings = {timezone=America/New_York, use_declarative_schema_changer=off, statement_timeout=10s}
//  2. newSettings = {timezone=America/New_York, statement_timeout=10s}
//  3. err = nil
func (n *alterRoleSetNode) makeNewSettings(
	params runParams, opName string, roleName username.SQLUsername,
) (oldSettings []string, newSettings []string, err error) {
	var selectQuery = fmt.Sprintf(
		`SELECT settings FROM %s WHERE database_id = $1 AND role_name = $2`,
		sessioninit.DatabaseRoleSettingsTableName,
	)
	datums, err := params.p.InternalSQLTxn().QueryRowEx(
		params.ctx,
		opName,
		params.p.txn,
		sessiondata.RootUserSessionDataOverride,
		selectQuery,
		n.dbDescID,
		roleName,
	)
	if err != nil {
		return nil, nil, err
	}
	if datums != nil {
		for _, s := range tree.MustBeDArray(datums[0]).Array {
			oldSetting := string(tree.MustBeDString(s))
			oldSettings = append(oldSettings, oldSetting)
			keyVal := strings.SplitN(oldSetting, "=", 2)
			if !strings.EqualFold(n.varName, keyVal[0]) {
				newSettings = append(newSettings, oldSetting)
			}
		}
	}
	return oldSettings, newSettings, nil
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
		d, err := eval.Expr(params.ctx, params.EvalContext(), v)
		if err != nil {
			return "", err
		}
		n.typedValues[i] = d
	}
	var strVal string
	var err error
	if n.sVar.GetStringVal != nil {
		strVal, err = n.sVar.GetStringVal(params.ctx, params.extendedEvalCtx, n.typedValues, params.p.Txn())
	} else {
		// No string converter defined, use the default one.
		strVal, err = getStringVal(params.ctx, params.EvalContext(), n.varName, n.typedValues)
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
