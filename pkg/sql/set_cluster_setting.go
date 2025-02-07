// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"context"
	"encoding/base64"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
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
	zeroInputPlanNode
	name    settings.SettingName
	st      *cluster.Settings
	setting settings.NonMaskedSetting
	// If value is nil, the setting should be reset.
	value tree.TypedExpr
}

func checkPrivilegesForSetting(
	ctx context.Context, p *planner, name settings.SettingName, action string,
) error {
	// If the user has modify privileges, then they can set or show any setting.
	hasModify, err := p.HasGlobalPrivilegeOrRoleOption(ctx, privilege.MODIFYCLUSTERSETTING)
	if err != nil {
		return err
	}
	if hasModify {
		return nil
	}

	// If the user only has sql modify privileges, then they can only set or show
	// any sql.defaults setting.
	isSqlSetting := strings.HasPrefix(string(name), "sql.defaults")
	if isSqlSetting {
		hasSqlModify, err := p.HasGlobalPrivilegeOrRoleOption(ctx, privilege.MODIFYSQLCLUSTERSETTING)
		if err != nil {
			return err
		}
		if hasSqlModify {
			return nil
		}
	}

	// If the user does not have modify or sql modify privileges, then they
	// cannot set any settings.
	if action == "set" {
		if !isSqlSetting {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				"only users with the %s privilege are allowed to %s cluster setting '%s'",
				privilege.MODIFYCLUSTERSETTING, action, name)
		}
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"only users with the %s or %s privilege are allowed to %s cluster setting '%s'",
			privilege.MODIFYCLUSTERSETTING, privilege.MODIFYSQLCLUSTERSETTING, action, name)
	}

	// If the user has view privileges, then they can show any setting.
	hasView, err := p.HasGlobalPrivilegeOrRoleOption(ctx, privilege.VIEWCLUSTERSETTING)
	if err != nil {
		return err
	}
	if action == "show" && hasView {
		return nil
	}

	// If the user does not have modify, sql modify, or view privileges,
	// then they cannot show any setting.
	if !isSqlSetting {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"only users with %s or %s privileges are allowed to %s cluster setting '%s'",
			privilege.MODIFYCLUSTERSETTING, privilege.VIEWCLUSTERSETTING, action, name)
	}
	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"only users with %s, %s or %s privileges are allowed to %s cluster setting '%s'",
		privilege.MODIFYCLUSTERSETTING, privilege.MODIFYSQLCLUSTERSETTING, privilege.VIEWCLUSTERSETTING, action, name)
}

// SetClusterSetting sets cluster settings.
// Privileges: super user.
func (p *planner) SetClusterSetting(
	ctx context.Context, n *tree.SetClusterSetting,
) (planNode, error) {
	forSystemTenant := p.ExecCfg().Codec.ForSystemTenant()
	tipSystemInterface := !forSystemTenant && TipUserAboutSystemInterface.Get(&p.ExecCfg().Settings.SV)
	name := settings.SettingName(strings.ToLower(n.Name))

	friendlyIgnore := func() {
		p.BufferClientNotice(ctx, errors.WithHintf(pgnotice.Newf("ignoring attempt to modify %q", name),
			"The setting is only modifiable by the operator.\n"+
				"Normally, an error would be reported, but the operation is silently accepted here as configured by %q.",
			TipUserAboutSystemInterface.Name()))
	}

	st := p.EvalContext().Settings
	setting, ok, nameStatus := settings.LookupForLocalAccess(name, forSystemTenant)
	if !ok {
		// Uh-oh.
		unknownSettingError := pgerror.Newf(pgcode.UndefinedParameter, "unknown cluster setting '%s'", name)

		// There's 3 cases here.
		//
		// - the setting does not exist. We'll fall back to a "unknown
		//   setting" error below.
		//
		// - the setting exists and was previously application. In this case,
		//   either report "unknown setting" in the common case, or, if
		//   the "tip" flag is enabled, _make the operation succeed
		//   as a no-op_ with a simple NOTICE.
		//
		// - the setting exists and has always been non-application. Either
		//   tell the user "unknown setting" in the common case, or, if the
		//   "tip" flag is enabled, tell the user "nope, connect to system
		//   interface instead".
		//
		//

		// Check if the setting exists at all, perhaps as a system setting.
		actualSetting, settingExists, _ := settings.LookupForLocalAccess(name, true /* forSystemTenant */)
		if !settingExists {
			return nil, unknownSettingError
		}

		// Did the setting previously have ApplicationLevel?
		if settings.SettingPreviouslyHadApplicationClass(actualSetting.InternalKey()) {
			if !tipSystemInterface {
				return nil, unknownSettingError
			}

			friendlyIgnore()
			return &zeroNode{}, nil
		}

		// This is a system setting.

		if !tipSystemInterface {
			return nil, unknownSettingError
		}

		return nil, p.maybeAddSystemInterfaceHint(
			pgerror.Newf(pgcode.InsufficientPrivilege, "cannot modify storage-level setting from virtual cluster"),
			"modify the cluster setting")
	}

	if nameStatus != settings.NameActive {
		p.BufferClientNotice(ctx, settingAlternateNameNotice(name, setting.Name()))
		name = setting.Name()
	}

	if err := checkPrivilegesForSetting(ctx, p, name, "set"); err != nil {
		return nil, err
	}

	if !forSystemTenant {
		switch setting.Class() {
		case settings.SystemOnly:
			// The Lookup call above should never return SystemOnly settings if this
			// is a tenant.
			return nil, errors.AssertionFailedf("looked up system-only setting")

		case settings.SystemVisible:
			// Did the setting previously have ApplicationLevel?
			if settings.SettingPreviouslyHadApplicationClass(setting.InternalKey()) && tipSystemInterface {
				friendlyIgnore()
				return &zeroNode{}, nil
			}

			return nil, p.maybeAddSystemInterfaceHint(
				pgerror.Newf(pgcode.InsufficientPrivilege, "setting %s is only settable by the operator", name),
				"modify the cluster setting")
		}
	} else {
		switch setting.Class() {
		case settings.ApplicationLevel:
			if err := p.shouldRestrictAccessToSystemInterface(ctx,
				"update to application-level cluster setting", /* operation */
				"changing the setting" /* alternate action */); err != nil {
				return nil, err
			}
		}
	}

	if st.OverridesInformer != nil && st.OverridesInformer.IsOverridden(setting.InternalKey()) {
		return nil, errors.Errorf("cluster setting '%s' is currently overridden by the operator", name)
	}

	value, err := p.getAndValidateTypedClusterSetting(ctx, name, n.Value, setting)
	if err != nil {
		return nil, err
	}

	csNode := setClusterSettingNode{
		name:    name,
		st:      st,
		setting: setting,
		value:   value,
	}
	return &csNode, nil
}

func (p *planner) getAndValidateTypedClusterSetting(
	ctx context.Context, name settings.SettingName, expr tree.Expr, setting settings.NonMaskedSetting,
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
			case settings.AnyEnumSetting:
				// EnumSettings can be set with either strings or integers.
				requiredType = types.AnyElement
			case *settings.DurationSetting:
				requiredType = types.Interval
			case *settings.DurationSettingWithExplicitUnit:
				requiredType = types.Interval
				// Ensure that the expression contains a unit (i.e can't be a float)
				_, err := p.analyzeExpr(
					ctx, expr, dummyHelper, types.Float, false, "SET CLUSTER SETTING "+string(name),
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
				ctx, expr, dummyHelper, requiredType, true, "SET CLUSTER SETTING "+string(name))
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
	if strings.HasPrefix(string(n.setting.InternalKey()), "sql.defaults") {
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
		params.p.logEvent,
		params.p.descCollection.ReleaseLeases,
		params.p.makeUnsafeSettingInterlockInfo(),
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
	name settings.SettingName,
	user username.SQLUsername,
	st *cluster.Settings,
	value tree.TypedExpr,
	evalCtx *eval.Context,
	logFn func(context.Context, descpb.ID, logpb.EventPayload) error,
	releaseLeases func(context.Context),
	interlockInfo unsafeSettingInterlockInfo,
) (expectedEncodedValue string, err error) {
	if err := func() error {
		var reportedValue string
		if value == nil {
			// This code is doing work for RESET CLUSTER SETTING.
			var err error
			reportedValue, expectedEncodedValue, err = writeDefaultSettingValue(ctx, db, setting)
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
				ctx, hook, db, setting, user, st, value, releaseLeases, interlockInfo,
			)
			if err != nil {
				return err
			}
		}

		if setting.IsUnsafe() {
			// Also mention the change in the non-structured DEV log.
			log.Warningf(ctx, "unsafe setting changed: %q -> %v", name, reportedValue)
		}

		return logFn(ctx,
			0, /* no target */
			&eventpb.SetClusterSetting{
				SettingName: string(name),
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
	ctx context.Context, db isql.DB, setting settings.NonMaskedSetting,
) (reportedValue string, expectedEncodedValue string, err error) {
	reportedValue = "DEFAULT"
	expectedEncodedValue = setting.EncodedDefault()
	_, err = db.Executor().ExecEx(
		ctx, "reset-setting", nil,
		sessiondata.NodeUserSessionDataOverride,
		"DELETE FROM system.settings WHERE name = $1", setting.InternalKey(),
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
	user username.SQLUsername,
	st *cluster.Settings,
	value tree.Datum,
	releaseLeases func(context.Context),
	interlockInfo unsafeSettingInterlockInfo,
) (reportedValue string, expectedEncodedValue string, err error) {
	// Stringify the value set by the statement for reporting in errors, logs etc.
	reportedValue = tree.AsStringWithFlags(value, tree.FmtBareStrings)

	// Validate the input and convert it to the binary encoding.
	encoded, err := toSettingString(ctx, st, setting, value)
	expectedEncodedValue = encoded
	if err != nil {
		return reportedValue, expectedEncodedValue, err
	}

	verSetting, isSetVersion := setting.(*settings.VersionSetting)
	if isSetVersion {
		if err := setVersionSetting(
			ctx, hook, verSetting, db, user, st, value, encoded, releaseLeases,
		); err != nil {
			return reportedValue, expectedEncodedValue, err
		}
	} else {
		// Modifying another setting than the version.
		if setting.IsUnsafe() {
			if err := unsafeSettingInterlock(ctx, st, setting, encoded, interlockInfo); err != nil {
				return reportedValue, expectedEncodedValue, err
			}
		}

		if _, err = db.Executor().ExecEx(
			ctx, "update-setting", nil,
			sessiondata.NodeUserSessionDataOverride,
			`UPSERT INTO system.settings (name, value, "lastUpdated", "valueType") VALUES ($1, $2, now(), $3)`,
			setting.InternalKey(), encoded, setting.Typ(),
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
	db isql.DB,
	user username.SQLUsername,
	st *cluster.Settings,
	value tree.Datum,
	encoded string,
	releaseLeases func(context.Context),
) error {
	// In the special case of the 'version' cluster setting,
	// we must first read the previous value to validate that the
	// value change is valid.
	datums, err := db.Executor().QueryRowEx(
		ctx, "retrieve-prev-setting", nil,
		sessiondata.NodeUserSessionDataOverride,
		"SELECT value FROM system.settings WHERE name = $1", setting.InternalKey(),
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
		return errors.New("no persisted cluster version found, please retry later")
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
	updateVersionSystemSetting := func(ctx context.Context, version clusterversion.ClusterVersion, postSettingValidate func(ctx context.Context, txn *kv.Txn) error) error {
		rawValue, err := protoutil.Marshal(&version)
		if err != nil {
			return err
		}
		return db.KV().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// On complex clusters with a large number of descriptors (> 500) and
			// multi-region nodes (> 9), normal priority transactions reading/updating
			// the version row can be starved. This is due to the lease manager reading
			// the version row at high priority, when refreshing leases (#95227), with
			// a complex cluster this traffic will continuous.
			// Run the version bump inside the upgrade as high priority, since
			// lease manager ends up reading the version row (with high priority)
			// inside the settings table when refreshing leases. On complex clusters
			// (multi-region with high latency) or with a large number of descriptors
			// ( >500) it's possible for normal transactions to be starved by continuous
			// lease traffic.
			// This is safe from deadlocks / starvation because we expected this
			// transaction only do the following:
			// 1) We expect this transaction to only read and write to the
			//    version key in the system.settings table. To achieve the smallest
			//    possible txn and avoid extra operations on other keys, we are going to
			//	  use KV call with EncodeSettingKey/EncodeSettingValue functions
			//	  instead of using the internal executor.
			// 2) Reads from the system.sql_instances table to confirm all SQL servers
			//    have been upgraded in multi-tenant environments.
			// 3) Other transactions will use a normal priority and get pushed out by
			//    this one, if they involve schema changes on the system database
			//    descriptor (highly unlikely).
			if err := txn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
				return err
			}

			// Fetch the existing version setting and see if its
			// been modified.
			codec := db.(*InternalDB).server.cfg.Codec
			decoder := settingswatcher.MakeRowDecoder(codec)
			key := settingswatcher.EncodeSettingKey(codec, "version")
			row, err := txn.Get(ctx, key)
			if err != nil {
				return err
			}
			if row.Value != nil {
				_, val, _, err := decoder.DecodeRow(roachpb.KeyValue{Key: row.Key, Value: *row.Value}, nil /* alloc */)
				if err != nil {
					return err
				}
				oldRawValue := []byte(val.Value)
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
			// Encode the setting value to write out the updated version.
			var tuple []byte
			if tuple, err = settingswatcher.EncodeSettingValue(rawValue, setting.Typ()); err != nil {
				return err
			}
			newValue := &roachpb.Value{}
			newValue.SetTuple(tuple)
			if err := txn.Put(ctx, row.Key, newValue); err != nil {
				return err
			}
			// Perform any necessary post-setting validation. This is used in
			// the tenant upgrade interlock to ensure that the set of sql
			// servers present at the time of the settings update, matches the
			// set that was present when the fence bump occurred (see comment in
			// upgrademanager.Migrate() for more details).
			if err = postSettingValidate(ctx, txn); err != nil {
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
	name settings.SettingName,
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
	ctx context.Context, st *cluster.Settings, s settings.Setting, d tree.Datum,
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
			if err := setting.Validate(&st.SV, bool(*b)); err != nil {
				return "", err
			}
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
	case settings.AnyEnumSetting:
		if i, intOK := d.(*tree.DInt); intOK {
			v, ok := setting.ParseEnum(settings.EncodeInt(int64(*i)))
			if ok {
				return settings.EncodeInt(v), nil
			}
			return "", errors.WithHint(errors.Errorf("invalid integer value '%d' for enum setting", *i), setting.GetAvailableValuesAsHint())
		} else if s, ok := d.(*tree.DString); ok {
			str := string(*s)
			v, ok := setting.ParseEnum(str)
			if ok {
				return settings.EncodeInt(v), nil
			}
			return "", errors.WithHint(errors.Errorf("invalid string value '%s' for enum setting", str), setting.GetAvailableValuesAsHint())
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

// unsafeSettingInterlockInfo contains information about the current
// session that is used by the unsafe setting interlock system.
type unsafeSettingInterlockInfo struct {
	sessionID    clusterunique.ID
	interlockKey string
}

func (p *planner) makeUnsafeSettingInterlockInfo() unsafeSettingInterlockInfo {
	return unsafeSettingInterlockInfo{
		sessionID:    p.ExtendedEvalContext().SessionID,
		interlockKey: p.SessionData().UnsafeSettingInterlockKey,
	}
}

const interlockKeySessionVarName = "unsafe_setting_interlock_key"

// unsafeSettingInterlock ensures that changes to unsafe settings are
// doubly confirmed by the operator by a special value in a session
// variable.
func unsafeSettingInterlock(
	ctx context.Context,
	st *cluster.Settings,
	setting settings.Setting,
	encodedValue string,
	info unsafeSettingInterlockInfo,
) error {
	// The interlock key is a combination of:
	// - the session ID, so that different sessions need different keys.
	// - the setting key, so that different settings need different
	//   interlock keys.
	h := fnv.New32()
	h.Write([]byte(info.sessionID.String()))
	h.Write([]byte(setting.InternalKey()))
	pastableKey := base64.StdEncoding.EncodeToString(h.Sum(nil))

	if info.interlockKey != pastableKey {
		return errors.WithDetailf(
			pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"changing cluster setting %q may cause cluster instability or data corruption.\n"+
					"To confirm the change, run the following command before trying again:\n\n"+
					"   SET %s = '%s';\n\n",
				setting.Name(), interlockKeySessionVarName, pastableKey,
			),
			"key: %s", pastableKey,
		)
	}
	return nil
}
