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
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/authentication"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/hintdetail"
)

// setClusterSettingNode represents a SET CLUSTER SETTING statement.
type setClusterSettingNode struct {
	name    string
	st      *cluster.Settings
	setting settings.WritableSetting
	// If value is nil, the setting should be reset.
	value tree.TypedExpr
	// versionUpgradeHook is called after validating a `SET CLUSTER SETTING
	// version` but before executing it. It can carry out arbitrary migrations
	// that allow us to eventually remove legacy code.
	versionUpgradeHook VersionUpgradeHook
}

func checkPrivilegesForSetting(ctx context.Context, p *planner, name string, action string) error {
	if settings.AdminOnly(name) {
		return p.RequireAdminRole(ctx, fmt.Sprintf("%s cluster setting '%s'", action, name))
	}
	hasModify, err := p.HasRoleOption(ctx, roleoption.MODIFYCLUSTERSETTING)
	if err != nil {
		return err
	}
	if !hasModify {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"only users with the %s privilege are allowed to %s cluster setting '%s'",
			roleoption.MODIFYCLUSTERSETTING, action, name)
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
	v, ok := settings.Lookup(name, settings.LookupForLocalAccess)
	if !ok {
		return nil, errors.Errorf("unknown cluster setting '%s'", name)
	}

	if err := checkPrivilegesForSetting(ctx, p, name, "set"); err != nil {
		return nil, err
	}

	setting, ok := v.(settings.WritableSetting)
	if !ok {
		return nil, errors.AssertionFailedf("expected writable setting, got %T", v)
	}

	if setting.SystemOnly() && !p.execCfg.Codec.ForSystemTenant() {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"setting %s is only settable in the system tenant", name)
	}

	var value tree.TypedExpr
	if n.Value != nil {
		// For DEFAULT, let the value reference be nil. That's a RESET in disguise.
		if _, ok := n.Value.(tree.DefaultVal); !ok {
			expr := n.Value
			expr = paramparse.UnresolvedNameToStrVal(expr)

			var requiredType *types.T
			var dummyHelper tree.IndexedVarHelper

			switch setting.(type) {
			case *settings.StringSetting, *settings.VersionSetting, *settings.ByteSizeSetting:
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

	csNode := setClusterSettingNode{
		name: name, st: st, setting: setting, value: value,
		versionUpgradeHook: p.execCfg.VersionUpgradeHook,
	}
	return &csNode, nil
}

func (n *setClusterSettingNode) startExec(params runParams) error {
	if !params.p.ExtendedEvalContext().TxnImplicit {
		return errors.Errorf("SET CLUSTER SETTING cannot be used inside a transaction")
	}
	execCfg := params.extendedEvalCtx.ExecCfg
	var expectedEncodedValue string
	if err := execCfg.DB.Txn(params.ctx, func(ctx context.Context, txn *kv.Txn) error {
		var reportedValue string
		if n.value == nil {
			reportedValue = "DEFAULT"
			expectedEncodedValue = n.setting.EncodedDefault()
			if _, err := execCfg.InternalExecutor.ExecEx(
				ctx, "reset-setting", txn,
				sessiondata.InternalExecutorOverride{User: security.RootUserName()},
				"DELETE FROM system.settings WHERE name = $1", n.name,
			); err != nil {
				return err
			}
		} else {
			value, err := n.value.Eval(params.p.EvalContext())
			if err != nil {
				return err
			}
			reportedValue = tree.AsStringWithFlags(value, tree.FmtBareStrings)
			var prev tree.Datum
			_, isSetVersion := n.setting.(*settings.VersionSetting)
			if isSetVersion {
				datums, err := execCfg.InternalExecutor.QueryRowEx(
					ctx, "retrieve-prev-setting", txn,
					sessiondata.InternalExecutorOverride{User: security.RootUserName()},
					"SELECT value FROM system.settings WHERE name = $1", n.name,
				)
				if err != nil {
					return err
				}
				if len(datums) == 0 {
					// There is a SQL migration which adds this value. If it
					// hasn't run yet, we can't update the version as we don't
					// have good enough information about the current cluster
					// version.
					if params.extendedEvalCtx.Codec.ForSystemTenant() {
						return errors.New("no persisted cluster version found, please retry later")
					}
					// The tenant cluster in 20.2 did not ever initialize this value and
					// utilized a hard-coded value of
					tenantDefaultVersion := clusterversion.ClusterVersion{
						Version: clusterversion.ByKey(clusterversion.V20_2),
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
			}
			encoded, err := toSettingString(ctx, n.st, n.name, n.setting, value, prev)
			expectedEncodedValue = encoded
			if err != nil {
				return err
			}

			if isSetVersion {
				if err := runVersionUpgradeHook(
					ctx, params, prev, value, n.versionUpgradeHook,
				); err != nil {
					return err
				}
			}

			if _, err = execCfg.InternalExecutor.ExecEx(
				ctx, "update-setting", txn,
				sessiondata.InternalExecutorOverride{User: security.RootUserName()},
				`UPSERT INTO system.settings (name, value, "lastUpdated", "valueType") VALUES ($1, $2, now(), $3)`,
				n.name, encoded, n.setting.Typ(),
			); err != nil {
				return err
			}

			if params.p.execCfg.TenantTestingKnobs != nil {
				if err := params.p.execCfg.TenantTestingKnobs.ClusterSettingsUpdater.Set(ctx, n.name, encoded, n.setting.Typ()); err != nil {
					return err
				}
			}
		}

		if n.name == authentication.CacheEnabledSettingName {
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
		case stats.AutoStatsClusterSettingName:
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
		}

		return params.p.logEvent(ctx,
			0, /* no target */
			&eventpb.SetClusterSetting{
				SettingName: n.name,
				Value:       reportedValue,
			})
	}); err != nil {
		return err
	}

	if _, ok := n.setting.(*settings.VersionSetting); ok && n.value == nil {
		// The "version" setting doesn't have a well defined "default" since it
		// is set in a startup migration.
		return nil
	}
	errNotReady := errors.New("setting updated but timed out waiting to read new value")
	var observed string
	err := retry.ForDuration(10*time.Second, func() error {
		observed = n.setting.Encoded(&execCfg.Settings.SV)
		if observed != expectedEncodedValue {
			return errNotReady
		}
		return nil
	})
	if err != nil {
		log.Warningf(
			params.ctx, "SET CLUSTER SETTING %q timed out waiting for value %q, observed %q",
			n.name, expectedEncodedValue, observed,
		)
	}
	return err
}

func runVersionUpgradeHook(
	ctx context.Context, params runParams, prev tree.Datum, value tree.Datum, f VersionUpgradeHook,
) error {
	var from, to clusterversion.ClusterVersion

	fromVersionVal := []byte(string(*prev.(*tree.DString)))
	if err := protoutil.Unmarshal(fromVersionVal, &from); err != nil {
		return err
	}

	targetVersionStr := string(*value.(*tree.DString))
	to.Version = roachpb.MustParseVersion(targetVersionStr)

	start21_1 := clusterversion.ByKey(clusterversion.Start21_1)
	if !params.extendedEvalCtx.Codec.ForSystemTenant() && from.Less(start21_1) {

		// In the case that we're setting the cluster version to something that
		// precedes the start of 21.1, which is permitted, if only because it's
		// complex to prevent, then there's definitely no migrations to run and
		// it may be hazardous due to assumptions about versions being even.
		if to.Less(start21_1) {
			return nil
		}
		// Otherwise, tell the migration layer that we're starting from the lowest
		// allowable version.
		from.Version = start21_1
	}

	// toSettingString already validated the input, and checked to
	// see that we are allowed to transition. Let's call into our
	// upgrade hook to run migrations, if any.
	if err := f(ctx, params.p.User(), from, to); err != nil {
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
//   current value of the setting, read from the system.settings table.
func toSettingString(
	ctx context.Context, st *cluster.Settings, name string, s settings.Setting, d, prev tree.Datum,
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
	case *settings.VersionSetting:
		if s, ok := d.(*tree.DString); ok {
			dStr, ok := prev.(*tree.DString)
			if !ok {
				return "", errors.Errorf("the existing value is not a string, got %T", prev)
			}

			prevRawVal := []byte(string(*dStr))
			newRawVal, err := clusterversion.EncodingFromVersionStr(string(*s))
			if err != nil {
				return "", err
			}

			newBytes, err := setting.Validate(ctx, &st.SV, prevRawVal, newRawVal)
			if err != nil {
				return "", err
			}
			return string(newBytes), nil
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
