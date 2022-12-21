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
	"bytes"
	"context"
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/hintdetail"
	"github.com/cockroachdb/redact"
)

// setClusterSettingNode represents a SET CLUSTER SETTING statement.
type setClusterSettingNode struct {
	name    string
	st      *cluster.Settings
	setting settings.NonMaskedSetting
	// If value is nil, the setting should be reset.
	value tree.TypedExpr
}

func checkPrivilegesForSetting(ctx context.Context, p *planner, name string, action string) error {

	// First check system privileges.
	hasModify := false
	hasSqlModify := false
	hasView := false
	if ok, err := p.HasPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.MODIFYCLUSTERSETTING, p.User()); err != nil {
		return err
	} else if ok {
		hasModify = true
		hasSqlModify = true
		hasView = true
	}
	if !hasSqlModify {
		if ok, err := p.HasPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.MODIFYSQLCLUSTERSETTING, p.User()); err != nil {
			return err
		} else if ok {
			hasSqlModify = true
			hasView = true
		}
	}
	if !hasView {
		if ok, err := p.HasPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWCLUSTERSETTING, p.User()); err != nil {
			return err
		} else if ok {
			hasView = true
		}
	}

	// Fallback to role option if the user doesn't have the privilege.
	if !hasModify {
		ok, err := p.HasRoleOption(ctx, roleoption.MODIFYCLUSTERSETTING)
		if err != nil {
			return err
		}
		hasModify = hasModify || ok
		hasSqlModify = hasSqlModify || ok
		hasView = hasView || ok
	}

	// The "set" action requires MODIFYCLUSTERSETTING or at least MODIFYSQLCLUSTERSETTING if
	// the setting is a sql.defaults setting.
	if action == "set" && !hasModify {
		isSqlSetting := strings.HasPrefix(name, "sql.defaults")
		if hasSqlModify && isSqlSetting {
			return nil
		} else if !isSqlSetting {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				"only users with the %s privilege are allowed to %s cluster setting '%s'",
				privilege.MODIFYCLUSTERSETTING, action, name)
		}
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"only users with the %s or %s privilege are allowed to %s cluster setting '%s'",
			privilege.MODIFYCLUSTERSETTING, privilege.MODIFYSQLCLUSTERSETTING, action, name)
	}

	if !hasView {
		ok, err := p.HasRoleOption(ctx, roleoption.VIEWCLUSTERSETTING)
		if err != nil {
			return err
		}
		hasView = hasView || ok
	}

	// The "show" action requires either either MODIFYCLUSTERSETTING or VIEWCLUSTERSETTING privileges.
	if action == "show" && !hasView {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"only users with %s, %s or %s privileges are allowed to %s cluster setting '%s'",
			privilege.MODIFYCLUSTERSETTING, privilege.MODIFYSQLCLUSTERSETTING, privilege.VIEWCLUSTERSETTING, action, name)
	}
	return nil
}

// SetClusterSetting sets session variables.
// Privileges: super user.
func (p *planner) SetClusterSetting(
	ctx context.Context, n *tree.SetClusterSetting,
) (planNode, error) {
	name := strings.ToLower(n.Name)
	st := p.EvalContext().Settings
	setting, ok := settings.LookupForLocalAccess(name, p.ExecCfg().Codec.ForSystemTenant())
	if !ok {
		return nil, errors.Errorf("unknown cluster setting '%s'", name)
	}

	if err := checkPrivilegesForSetting(ctx, p, name, "set"); err != nil {
		return nil, err
	}

	if !p.execCfg.Codec.ForSystemTenant() {
		switch setting.Class() {
		case settings.SystemOnly:
			// The Lookup call above should never return SystemOnly settings if this
			// is a tenant.
			return nil, errors.AssertionFailedf("looked up system-only setting")
		case settings.TenantReadOnly:
			return nil, pgerror.Newf(pgcode.InsufficientPrivilege, "setting %s is only settable by the operator", name)
		}
	}

	if st.OverridesInformer != nil && st.OverridesInformer.IsOverridden(name) {
		return nil, errors.Errorf("cluster setting '%s' is currently overridden by the operator", name)
	}

	value, err := p.getAndValidateTypedClusterSetting(ctx, name, n.Value, setting)
	if err != nil {
		return nil, err
	}

	csNode := setClusterSettingNode{
		name: name, st: st, setting: setting, value: value,
	}
	return &csNode, nil
}

func (p *planner) getAndValidateTypedClusterSetting(
	ctx context.Context, name string, expr tree.Expr, setting settings.NonMaskedSetting,
) (tree.TypedExpr, error) {
	var value tree.TypedExpr
	if expr != nil {
		// For DEFAULT, let the value reference be nil. That's a RESET in disguise.
		if _, ok := expr.(tree.DefaultVal); !ok {
			expr = paramparse.UnresolvedNameToStrVal(expr)

			var requiredType *types.T
			var dummyHelper tree.IndexedVarHelper

			switch setting.(type) {
			case *settings.StringSetting, *settings.VersionSetting, *settings.ByteSizeSetting, *settings.ProtobufSetting:
				requiredType = types.String
			case *settings.BoolSetting:
				requiredType = types.Bool
			case *settings.IntSetting:
				requiredType = types.Int
			case *settings.FloatSetting:
				requiredType = types.Float
			case *settings.EnumSetting:
				requiredType = types.Any
			case *settings.DurationSetting:
				requiredType = types.Interval
			case *settings.DurationSettingWithExplicitUnit:
				requiredType = types.Interval
				// Ensure that the expression contains a unit (i.e can't be a float)
				_, err := p.analyzeExpr(
					ctx, expr, nil, dummyHelper, types.Float, false, "SET CLUSTER SETTING "+name,
				)
				// An interval with a unit (valid) will return an
				// "InvalidTextRepresentation" error when trying to parse it as a float.
				// If we don't get an error => No unit was present. Values that can't be
				// parsed as intervals will continue to be caught below using
				// requiredType.
				if err == nil {
					_, hint := setting.ErrorHint()
					return nil, hintdetail.WithHint(errors.New("invalid cluster setting argument type"), hint)
				}
			default:
				return nil, errors.Errorf("unsupported setting type %T", setting)
			}

			typed, err := p.analyzeExpr(
				ctx, expr, nil, dummyHelper, requiredType, true, "SET CLUSTER SETTING "+name)
			if err != nil {
				hasHint, hint := setting.ErrorHint()
				if hasHint {
					return nil, hintdetail.WithHint(err, hint)
				}
				return nil, err
			}
			value = typed
		} else if _, isVersionSetting := setting.(*settings.VersionSetting); isVersionSetting {
			return nil, errors.New("cannot RESET cluster version setting")
		}
	}
	return value, nil
}

func (n *setClusterSettingNode) startExec(params runParams) error {

	if strings.HasPrefix(n.name, "sql.defaults") {
		params.p.BufferClientNotice(
			params.ctx,
			errors.WithHintf(
				pgnotice.Newf("setting global default %s is not recommended", n.name),
				"use the `ALTER ROLE ... SET` syntax to control session variable defaults at a finer-grained level. See: %s",
				docs.URL("alter-role.html#set-default-session-variable-values-for-a-role"),
			),
		)
	}

	if !params.extendedEvalCtx.TxnIsSingleStmt {
		return errors.Errorf("SET CLUSTER SETTING cannot be used inside a multi-statement transaction")
	}

	expectedEncodedValue, err := writeSettingInternal(
		params.ctx,
		params.extendedEvalCtx.ExecCfg.VersionUpgradeHook,
		params.extendedEvalCtx.ExecCfg.InternalDB,
		n.setting, n.name,
		params.p.User(),
		n.st,
		n.value,
		params.p.EvalContext(),
		params.extendedEvalCtx.Codec.ForSystemTenant(),
		params.p.logEvent,
		params.p.descCollection.ReleaseLeases,
	)
	if err != nil {
		return err
	}

	if n.name == sessioninit.CacheEnabledSettingName {
		if expectedEncodedValue == "false" {
			// Bump role-related table versions to force other nodes to clear out
			// their AuthInfo cache.
			if err := params.p.bumpUsersTableVersion(params.ctx); err != nil {
				return err
			}
			if err := params.p.bumpRoleOptionsTableVersion(params.ctx); err != nil {
				return err
			}
		}
	}

	// Report tracked cluster settings via telemetry.
	// TODO(justin): implement a more general mechanism for tracking these.
	switch n.name {
	case catpb.AutoStatsEnabledSettingName:
		switch expectedEncodedValue {
		case "true":
			telemetry.Inc(sqltelemetry.TurnAutoStatsOnUseCounter)
		case "false":
			telemetry.Inc(sqltelemetry.TurnAutoStatsOffUseCounter)
		}
	case ConnAuditingClusterSettingName:
		switch expectedEncodedValue {
		case "true":
			telemetry.Inc(sqltelemetry.TurnConnAuditingOnUseCounter)
		case "false":
			telemetry.Inc(sqltelemetry.TurnConnAuditingOffUseCounter)
		}
	case AuthAuditingClusterSettingName:
		switch expectedEncodedValue {
		case "true":
			telemetry.Inc(sqltelemetry.TurnAuthAuditingOnUseCounter)
		case "false":
			telemetry.Inc(sqltelemetry.TurnAuthAuditingOffUseCounter)
		}
	case ReorderJoinsLimitClusterSettingName:
		val, err := strconv.ParseInt(expectedEncodedValue, 10, 64)
		if err != nil {
			break
		}
		sqltelemetry.ReportJoinReorderLimit(int(val))
	case VectorizeClusterSettingName:
		val, err := strconv.Atoi(expectedEncodedValue)
		if err != nil {
			break
		}
		validatedExecMode, isValid := sessiondatapb.VectorizeExecModeFromString(sessiondatapb.VectorizeExecMode(val).String())
		if !isValid {
			break
		}
		telemetry.Inc(sqltelemetry.VecModeCounter(validatedExecMode.String()))
	case colexec.HashAggregationDiskSpillingEnabledSettingName:
		if expectedEncodedValue == "false" {
			telemetry.Inc(sqltelemetry.HashAggregationDiskSpillingDisabled)
		}
	}
	// Don't bother waiting if we're ignoring the values; just say so now.
	if settings.IsIgnoringAllUpdates() {
		return errors.New("setting updated but will not take effect in this process due to manual override")
	}
	return waitForSettingUpdate(params.ctx, params.extendedEvalCtx.ExecCfg,
		n.setting, n.value == nil /* reset */, n.name, expectedEncodedValue)
}

func writeSettingInternal(
	ctx context.Context,
	hook VersionUpgradeHook,
	db isql.DB,
	setting settings.NonMaskedSetting,
	name string,
	user username.SQLUsername,
	st *cluster.Settings,
	value tree.TypedExpr,
	evalCtx *eval.Context,
	forSystemTenant bool,
	logFn func(context.Context, descpb.ID, logpb.EventPayload) error,
	releaseLeases func(context.Context),
) (expectedEncodedValue string, err error) {
	if err := func() error {
		var reportedValue string
		if value == nil {
			// This code is doing work for RESET CLUSTER SETTING.
			var err error
			reportedValue, expectedEncodedValue, err = writeDefaultSettingValue(ctx, db, setting, name)
			if err != nil {
				return err
			}
		} else {

			// Setting a non-DEFAULT value.
			value, err := eval.Expr(ctx, evalCtx, value)
			if err != nil {
				return err
			}
			reportedValue, expectedEncodedValue, err = writeNonDefaultSettingValue(
				ctx, hook, db,
				setting, name, user, st, value, forSystemTenant,
				releaseLeases,
			)
			if err != nil {
				return err
			}
		}
		return logFn(ctx,
			0, /* no target */
			&eventpb.SetClusterSetting{
				SettingName: name,
				Value:       reportedValue,
			})
	}(); err != nil {
		return "", err
	}

	return expectedEncodedValue, nil
}

// writeDefaultSettingValue performs the data write corresponding to a
// RESET CLUSTER SETTING statement or changing the value of a setting
// to DEFAULT.
func writeDefaultSettingValue(
	ctx context.Context, db isql.DB, setting settings.NonMaskedSetting, name string,
) (reportedValue string, expectedEncodedValue string, err error) {
	reportedValue = "DEFAULT"
	expectedEncodedValue = setting.EncodedDefault()
	_, err = db.Executor().ExecEx(
		ctx, "reset-setting", nil,
		sessiondata.RootUserSessionDataOverride,
		"DELETE FROM system.settings WHERE name = $1", name,
	)
	return reportedValue, expectedEncodedValue, err
}

// writeDefaultSettingValue performs the data write to change a
// setting to a non-DEFAULT value.
func writeNonDefaultSettingValue(
	ctx context.Context,
	hook VersionUpgradeHook,
	db isql.DB,
	setting settings.NonMaskedSetting,
	name string,
	user username.SQLUsername,
	st *cluster.Settings,
	value tree.Datum,
	forSystemTenant bool,
	releaseLeases func(context.Context),
) (reportedValue string, expectedEncodedValue string, err error) {
	// Stringify the value set by the statement for reporting in errors, logs etc.
	reportedValue = tree.AsStringWithFlags(value, tree.FmtBareStrings)

	// Validate the input and convert it to the binary encoding.
	encoded, err := toSettingString(ctx, st, name, setting, value)
	expectedEncodedValue = encoded
	if err != nil {
		return reportedValue, expectedEncodedValue, err
	}

	verSetting, isSetVersion := setting.(*settings.VersionSetting)
	if isSetVersion {
		if err := setVersionSetting(
			ctx, hook, verSetting, name, db, user, st, value, encoded,
			forSystemTenant, releaseLeases,
		); err != nil {
			return reportedValue, expectedEncodedValue, err
		}
	} else {
		// Modifying another setting than the version.
		if _, err = db.Executor().ExecEx(
			ctx, "update-setting", nil,
			sessiondata.RootUserSessionDataOverride,
			`UPSERT INTO system.settings (name, value, "lastUpdated", "valueType") VALUES ($1, $2, now(), $3)`,
			name, encoded, setting.Typ(),
		); err != nil {
			return reportedValue, expectedEncodedValue, err
		}
	}

	return reportedValue, expectedEncodedValue, nil
}

// setVersionSetting encapsulates the logic for changing the 'version'
// cluster setting.
func setVersionSetting(
	ctx context.Context,
	hook VersionUpgradeHook,
	setting *settings.VersionSetting,
	name string,
	db isql.DB,
	user username.SQLUsername,
	st *cluster.Settings,
	value tree.Datum,
	encoded string,
	forSystemTenant bool,
	releaseLeases func(context.Context),
) error {
	// In the special case of the 'version' cluster setting,
	// we must first read the previous value to validate that the
	// value change is valid.
	datums, err := db.Executor().QueryRowEx(
		ctx, "retrieve-prev-setting", nil,
		sessiondata.RootUserSessionDataOverride,
		"SELECT value FROM system.settings WHERE name = $1", name,
	)
	if err != nil {
		return err
	}
	var prev tree.Datum
	if len(datums) == 0 {
		// There is a SQL migration which adds this value. If it
		// hasn't run yet, we can't update the version as we don't
		// have good enough information about the current cluster
		// version.
		if forSystemTenant {
			return errors.New("no persisted cluster version found, please retry later")
		}
		// The tenant cluster in 20.2 did not ever initialize this value and
		// utilized this hard-coded value instead. In 21.1, the builtin
		// which creates tenants sets up the cluster version state. It also
		// is set when the version is upgraded.
		tenantDefaultVersion := clusterversion.ClusterVersion{
			Version: roachpb.Version{Major: 20, Minor: 2},
		}
		// Pretend that the expected value was already there to allow us to
		// run migrations.
		prevEncoded, err := protoutil.Marshal(&tenantDefaultVersion)
		if err != nil {
			return errors.WithAssertionFailure(err)
		}
		prev = tree.NewDString(string(prevEncoded))
	} else {
		prev = datums[0]
	}

	// Validate that the upgrade to the new version is valid.
	dStr, ok := prev.(*tree.DString)
	if !ok {
		return errors.Errorf("the existing value is not a string, got %T", prev)
	}
	if err := setting.Validate(ctx, &st.SV, []byte(string(*dStr)), []byte(encoded)); err != nil {
		return err
	}

	// Updates the version inside the system.settings table.
	// If we are already at or above the target version, then this
	// function is idempotent.
	updateVersionSystemSetting := func(ctx context.Context, version clusterversion.ClusterVersion, postSettingValidate func(ctx context.Context) error) error {
		rawValue, err := protoutil.Marshal(&version)
		if err != nil {
			return err
		}
		return db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			// Confirm if the version has actually changed on us.
			datums, err := txn.QueryRowEx(
				ctx, "retrieve-prev-setting", txn.KV(),
				sessiondata.RootUserSessionDataOverride,
				"SELECT value FROM system.settings WHERE name = $1", name,
			)
			if err != nil {
				return err
			}
			if len(datums) > 0 {
				dStr, ok := datums[0].(*tree.DString)
				if !ok {
					return errors.AssertionFailedf("existing version value is not a string, got %T", datums[0])
				}
				oldRawValue := []byte(string(*dStr))
				if bytes.Equal(oldRawValue, rawValue) {
					return nil
				}
				var oldValue clusterversion.ClusterVersion
				// If there is a pre-existing value, we ought to be able to decode it.
				if err := protoutil.Unmarshal(oldRawValue, &oldValue); err != nil {
					return errors.NewAssertionErrorWithWrappedErrf(err, "decoding previous version %s",
						redact.SafeString(base64.StdEncoding.EncodeToString(oldRawValue)))
				}
				if !oldValue.Less(version.Version) {
					return nil
				}
			}
			// Only if the version has increased, alter the setting.
			if _, err = txn.ExecEx(
				ctx, "update-setting", txn.KV(),
				sessiondata.RootUserSessionDataOverride,
				`UPSERT INTO system.settings (name, value, "lastUpdated", "valueType") VALUES ($1, $2, now(), $3)`,
				name, string(rawValue), setting.Typ(),
			); err != nil {
				return err
			}

			// If we're the system tenant, also send an override to each tenant
			// to ensure that they know about the new cluster version.
			if forSystemTenant {
				if _, err = txn.ExecEx(
					ctx, "update-setting", txn.KV(),
					sessiondata.RootUserSessionDataOverride,
					`UPSERT INTO system.tenant_settings (tenant_id, name, value, "last_updated", "value_type") VALUES ($1, $2, $3, now(), $4)`,
					tree.NewDInt(0), name, string(rawValue), setting.Typ(),
				); err != nil {
					return err
				}
			}

			// Perform any necessary post-setting validation. This is used in
			// the tenant upgrade interlock to ensure that the set of sql
			// servers present at the time of the settings update, matches the
			// set that was present when the fence bump occurred (see comment in
			// upgrademanager.Migrate() for more details).
			if err = postSettingValidate(ctx); err != nil {
				return err
			}

			return err
		})
	}

	// If we're here, we know we aren't in a transaction because we don't
	// allow setting cluster settings in a transaction. We need to release
	// our leases because the underlying migrations may affect descriptors
	// we currently have a lease for. We don't need to hold on to any leases
	// because the code isn't relying on them.
	releaseLeases(ctx)
	return runMigrationsAndUpgradeVersion(
		ctx, hook, user, prev, value, updateVersionSystemSetting,
	)
}

// waitForSettingUpdate makes the SET CLUSTER SETTING statement wait
// until the value has propagated and is visible to the current SQL
// session.
func waitForSettingUpdate(
	ctx context.Context,
	execCfg *ExecutorConfig,
	setting settings.NonMaskedSetting,
	reset bool,
	name string,
	expectedEncodedValue string,
) error {
	if _, ok := setting.(*settings.VersionSetting); ok && reset {
		// The "version" setting doesn't have a well defined "default" since it
		// is set in a startup migration.
		return nil
	}
	errNotReady := errors.New("setting updated but timed out waiting to read new value")
	var observed string
	err := retry.ForDuration(10*time.Second, func() error {
		observed = setting.Encoded(&execCfg.Settings.SV)
		if observed != expectedEncodedValue {
			return errNotReady
		}
		return nil
	})
	if err != nil {
		log.Warningf(
			ctx, "SET CLUSTER SETTING %q timed out waiting for value %q, observed %q",
			name, expectedEncodedValue, observed,
		)
	}
	return err
}

// runMigrationsAndUpgradeVersion runs migrations for the target version via
// execCfg.VersionUpgradeHook and then finalizes the change by calling
// updateVersionSystemSetting() to effectively write the new value to
// the system table.
func runMigrationsAndUpgradeVersion(
	ctx context.Context,
	hook VersionUpgradeHook,
	user username.SQLUsername,
	prev tree.Datum,
	value tree.Datum,
	updateVersionSystemSetting UpdateVersionSystemSettingHook,
) error {
	var from, to clusterversion.ClusterVersion

	fromVersionVal := []byte(string(*prev.(*tree.DString)))
	if err := protoutil.Unmarshal(fromVersionVal, &from); err != nil {
		return err
	}

	targetVersionStr := string(*value.(*tree.DString))
	to.Version = roachpb.MustParseVersion(targetVersionStr)

	// toSettingString already validated the input, and checked to
	// see that we are allowed to transition. Let's call into our
	// upgrade hook to run migrations, if any.
	if err := hook(ctx, user, from, to, updateVersionSystemSetting); err != nil {
		return err
	}
	return nil
}

func (n *setClusterSettingNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *setClusterSettingNode) Values() tree.Datums            { return nil }
func (n *setClusterSettingNode) Close(_ context.Context)        {}

// toSettingString takes in a datum that's supposed to become the value for a
// Setting and validates it, returning the string representation of the new
// value as it needs to be inserted into the system.settings table.
//
// Args:
// prev: Only specified if the setting is a StateMachineSetting. Represents the
//
//	current value of the setting, read from the system.settings table.
func toSettingString(
	ctx context.Context, st *cluster.Settings, name string, s settings.Setting, d tree.Datum,
) (string, error) {
	switch setting := s.(type) {
	case *settings.StringSetting:
		if s, ok := d.(*tree.DString); ok {
			if err := setting.Validate(&st.SV, string(*s)); err != nil {
				return "", err
			}
			return string(*s), nil
		}
		return "", errors.Errorf("cannot use %s %T value for string setting", d.ResolvedType(), d)
	case *settings.ProtobufSetting:
		if s, ok := d.(*tree.DString); ok {
			msg, err := setting.UnmarshalFromJSON(string(*s))
			if err != nil {
				return "", err
			}
			if err := setting.Validate(&st.SV, msg); err != nil {
				return "", err
			}
			return settings.EncodeProtobuf(msg), nil
		}
		return "", errors.Errorf("cannot use %s %T value for protobuf setting", d.ResolvedType(), d)
	case *settings.VersionSetting:
		if s, ok := d.(*tree.DString); ok {
			newRawVal, err := clusterversion.EncodingFromVersionStr(string(*s))
			if err != nil {
				return "", err
			}
			return string(newRawVal), nil
		}
		return "", errors.Errorf("cannot use %s %T value for string setting", d.ResolvedType(), d)
	case *settings.BoolSetting:
		if b, ok := d.(*tree.DBool); ok {
			return settings.EncodeBool(bool(*b)), nil
		}
		return "", errors.Errorf("cannot use %s %T value for bool setting", d.ResolvedType(), d)
	case *settings.IntSetting:
		if i, ok := d.(*tree.DInt); ok {
			if err := setting.Validate(int64(*i)); err != nil {
				return "", err
			}
			return settings.EncodeInt(int64(*i)), nil
		}
		return "", errors.Errorf("cannot use %s %T value for int setting", d.ResolvedType(), d)
	case *settings.FloatSetting:
		if f, ok := d.(*tree.DFloat); ok {
			if err := setting.Validate(float64(*f)); err != nil {
				return "", err
			}
			return settings.EncodeFloat(float64(*f)), nil
		}
		return "", errors.Errorf("cannot use %s %T value for float setting", d.ResolvedType(), d)
	case *settings.EnumSetting:
		if i, intOK := d.(*tree.DInt); intOK {
			v, ok := setting.ParseEnum(settings.EncodeInt(int64(*i)))
			if ok {
				return settings.EncodeInt(v), nil
			}
			return "", errors.WithHintf(errors.Errorf("invalid integer value '%d' for enum setting", *i), setting.GetAvailableValuesAsHint())
		} else if s, ok := d.(*tree.DString); ok {
			str := string(*s)
			v, ok := setting.ParseEnum(str)
			if ok {
				return settings.EncodeInt(v), nil
			}
			return "", errors.WithHintf(errors.Errorf("invalid string value '%s' for enum setting", str), setting.GetAvailableValuesAsHint())
		}
		return "", errors.Errorf("cannot use %s %T value for enum setting, must be int or string", d.ResolvedType(), d)
	case *settings.ByteSizeSetting:
		if s, ok := d.(*tree.DString); ok {
			bytes, err := humanizeutil.ParseBytes(string(*s))
			if err != nil {
				return "", err
			}
			if err := setting.Validate(bytes); err != nil {
				return "", err
			}
			return settings.EncodeInt(bytes), nil
		}
		return "", errors.Errorf("cannot use %s %T value for byte size setting", d.ResolvedType(), d)
	case *settings.DurationSetting:
		if f, ok := d.(*tree.DInterval); ok {
			if f.Duration.Months > 0 || f.Duration.Days > 0 {
				return "", errors.Errorf("cannot use day or month specifiers: %s", d.String())
			}
			d := time.Duration(f.Duration.Nanos()) * time.Nanosecond
			if err := setting.Validate(d); err != nil {
				return "", err
			}
			return settings.EncodeDuration(d), nil
		}
		return "", errors.Errorf("cannot use %s %T value for duration setting", d.ResolvedType(), d)
	case *settings.DurationSettingWithExplicitUnit:
		if f, ok := d.(*tree.DInterval); ok {
			if f.Duration.Months > 0 || f.Duration.Days > 0 {
				return "", errors.Errorf("cannot use day or month specifiers: %s", d.String())
			}
			d := time.Duration(f.Duration.Nanos()) * time.Nanosecond
			if err := setting.Validate(d); err != nil {
				return "", err
			}
			return settings.EncodeDuration(d), nil
		}
		return "", errors.Errorf("cannot use %s %T value for duration setting", d.ResolvedType(), d)
	default:
		return "", errors.Errorf("unsupported setting type %T", setting)
	}
}
