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
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
)

// setClusterSettingNode represents a SET CLUSTER SETTING statement.
type setClusterSettingNode struct {
	name    string
	st      *cluster.Settings
	setting settings.Setting
	// If value is nil, the setting should be reset.
	value tree.TypedExpr
}

// SetClusterSetting sets session variables.
// Privileges: super user.
func (p *planner) SetClusterSetting(
	ctx context.Context, n *tree.SetClusterSetting,
) (planNode, error) {
	if err := p.RequireSuperUser(ctx, "SET CLUSTER SETTING"); err != nil {
		return nil, err
	}

	name := strings.ToLower(n.Name)
	st := p.EvalContext().Settings
	setting, ok := settings.Lookup(name)
	if !ok {
		return nil, errors.Errorf("unknown cluster setting '%s'", name)
	}

	var value tree.TypedExpr
	if n.Value != nil {
		// For DEFAULT, let the value reference be nil. That's a RESET in disguise.
		if _, ok := n.Value.(tree.DefaultVal); !ok {
			expr := n.Value
			if s, ok := expr.(*tree.UnresolvedName); ok {
				// Special rule for SET: because SET doesn't apply in the context
				// of a table, SET ... = IDENT really means SET ... = 'IDENT'.
				expr = tree.NewStrVal(tree.AsStringWithFlags(s, tree.FmtBareIdentifiers))
			}

			var requiredType types.T
			switch setting.(type) {
			case *settings.StringSetting, *settings.StateMachineSetting, *settings.ByteSizeSetting:
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
			default:
				return nil, errors.Errorf("unsupported setting type %T", setting)
			}

			var dummyHelper tree.IndexedVarHelper
			typed, err := p.analyzeExpr(
				ctx, expr, nil, dummyHelper, requiredType, true, "SET CLUSTER SETTING "+name)
			if err != nil {
				return nil, err
			}

			value = typed
		}
	}

	return &setClusterSettingNode{name: name, st: st, setting: setting, value: value}, nil
}

func (n *setClusterSettingNode) startExec(params runParams) error {
	ie := InternalExecutor{ExecCfg: params.extendedEvalCtx.ExecCfg}

	var reportedValue string
	if n.value == nil {
		if _, err := ie.ExecuteStatementInTransaction(
			params.ctx, "reset-setting", params.p.txn,
			"DELETE FROM system.public.settings WHERE name = $1", n.name,
		); err != nil {
			return err
		}
		reportedValue = "DEFAULT"
	} else {
		// TODO(dt): validate and properly encode str according to type.
		encoded, err := params.p.toSettingString(params.ctx, ie, n.st, n.name, n.setting, n.value)
		if err != nil {
			return err
		}
		if _, err := ie.ExecuteStatementInTransaction(
			params.ctx, "update-setting", params.p.txn,
			`UPSERT INTO system.public.settings (name, value, "lastUpdated", "valueType") VALUES ($1, $2, NOW(), $3)`,
			n.name, encoded, n.setting.Typ(),
		); err != nil {
			return err
		}
		reportedValue = tree.AsStringWithFlags(n.value, tree.FmtBareStrings)
	}

	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogSetClusterSetting,
		0, /* no target */
		int32(params.extendedEvalCtx.NodeID),
		struct {
			SettingName string
			Value       string
			User        string
		}{n.name, reportedValue, params.SessionData().User},
	)
}

func (n *setClusterSettingNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *setClusterSettingNode) Values() tree.Datums            { return nil }
func (n *setClusterSettingNode) Close(_ context.Context)        {}

func (p *planner) toSettingString(
	ctx context.Context,
	ie InternalExecutor,
	st *cluster.Settings,
	name string,
	setting settings.Setting,
	val tree.TypedExpr,
) (string, error) {
	d, err := val.Eval(p.EvalContext())
	if err != nil {
		return "", err
	}

	switch setting := setting.(type) {
	case *settings.StringSetting:
		if s, ok := d.(*tree.DString); ok {
			if err := setting.Validate(string(*s)); err != nil {
				return "", err
			}
			return string(*s), nil
		}
		return "", errors.Errorf("cannot use %s %T value for string setting", d.ResolvedType(), d)
	case *settings.StateMachineSetting:
		if s, ok := d.(*tree.DString); ok {
			datums, err := ie.QueryRowInTransaction(
				ctx, "retrieve-prev-setting", p.txn, "SELECT value FROM system.public.settings WHERE name = $1", name,
			)
			if err != nil {
				return "", err
			}
			if len(datums) == 0 {
				// There is a SQL migration which adds this value. If it
				// hasn't run yet, we can't update the version as we don't
				// have good enough information about the current cluster
				// version.
				return "", errors.New("no persisted cluster version found, please retry later")
			}

			dStr, ok := datums[0].(*tree.DString)
			if !ok {
				return "", errors.New("the existing value is not a string")
			}
			prevRawVal := []byte(string(*dStr))
			newBytes, _, err := setting.Validate(&st.SV, prevRawVal, (*string)(s))
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
			return "", errors.Errorf("invalid integer value '%d' for enum setting", *i)
		} else if s, ok := d.(*tree.DString); ok {
			str := string(*s)
			v, ok := setting.ParseEnum(str)
			if ok {
				return settings.EncodeInt(v), nil
			}
			return "", errors.Errorf("invalid string value '%s' for enum setting", str)
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
			d := time.Duration(f.Duration.Nanos) * time.Nanosecond
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
