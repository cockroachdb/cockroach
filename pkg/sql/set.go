// Copyright 2015 The Cockroach Authors.
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
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// setNode represents a SET SESSION statement.
type setNode struct {
	v sessionVar
	// typedValues == nil means RESET.
	typedValues []parser.TypedExpr
}

// SetVar sets session variables.
// Privileges: None.
//   Notes: postgres/mysql do not require privileges for session variables (some exceptions).
func (p *planner) SetVar(ctx context.Context, n *parser.SetVar) (planNode, error) {
	if n.Name == nil {
		// A client has sent the reserved internal syntax SET ROW ...
		// Reject it.
		return nil, errors.New("invalid statement: SET ROW")
	}

	name := strings.ToLower(parser.AsStringWithFlags(n.Name, parser.FmtBareIdentifiers))

	var typedValues []parser.TypedExpr
	if len(n.Values) > 0 {
		isReset := false
		if len(n.Values) == 1 {
			if _, ok := n.Values[0].(parser.DefaultVal); ok {
				// "SET var = DEFAULT" means RESET.
				// In that case, we want typedValues to remain nil, so that
				// the Start() logic recognizes the RESET too.
				isReset = true
			}
		}

		if !isReset {
			typedValues = make([]parser.TypedExpr, len(n.Values))
			for i, expr := range n.Values {
				// Special rule for SET: because SET doesn't apply in the context
				// of a table, SET ... = IDENT really means SET ... = 'IDENT'.
				if s, ok := expr.(parser.UnresolvedName); ok {
					expr = parser.NewStrVal(parser.AsStringWithFlags(s, parser.FmtBareIdentifiers))
				}

				var dummyHelper parser.IndexedVarHelper
				typedValue, err := p.analyzeExpr(
					ctx, expr, nil, dummyHelper, parser.TypeString, false, "SET SESSION "+name)
				if err != nil {
					return nil, err
				}
				typedValues[i] = typedValue
			}
		}
	}

	v, ok := varGen[name]
	if !ok {
		return nil, fmt.Errorf("unknown variable: %q", name)
	}

	if typedValues != nil {
		if v.Set == nil {
			return nil, fmt.Errorf("variable \"%s\" cannot be changed", name)
		}
	} else {
		if v.Reset == nil {
			return nil, fmt.Errorf("variable \"%s\" cannot be reset", name)
		}
	}

	return &setNode{v: v, typedValues: typedValues}, nil
}

func (n *setNode) Start(params runParams) error {
	if n.typedValues != nil {
		return n.v.Set(params.ctx, params.p.session, n.typedValues)
	}
	return n.v.Reset(params.p.session)
}

func (n *setNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *setNode) Values() parser.Datums          { return nil }
func (n *setNode) Close(_ context.Context)        {}

// setClusterSettingNode represents a SET CLUSTER SETTING statement.
type setClusterSettingNode struct {
	name    string
	st      *cluster.Settings
	setting settings.Setting
	// If value is nil, the setting should be reset.
	value parser.TypedExpr
}

// SetClusterSetting sets session variables.
// Privileges: super user.
func (p *planner) SetClusterSetting(
	ctx context.Context, n *parser.SetClusterSetting,
) (planNode, error) {
	if err := p.RequireSuperUser("SET CLUSTER SETTING"); err != nil {
		return nil, err
	}

	name := strings.ToLower(parser.AsStringWithFlags(n.Name, parser.FmtBareIdentifiers))
	st := p.session.execCfg.Settings
	setting, ok := settings.Lookup(name)
	if !ok {
		return nil, errors.Errorf("unknown cluster setting '%s'", name)
	}

	var value parser.TypedExpr
	if n.Value != nil {
		// For DEFAULT, let the value reference be nil. That's a RESET in disguise.
		if _, ok := n.Value.(parser.DefaultVal); !ok {
			expr := n.Value
			if s, ok := expr.(parser.UnresolvedName); ok {
				// Special rule for SET: because SET doesn't apply in the context
				// of a table, SET ... = IDENT really means SET ... = 'IDENT'.
				expr = parser.NewStrVal(parser.AsStringWithFlags(s, parser.FmtBareIdentifiers))
			}

			var requiredType parser.Type
			switch setting.(type) {
			case *settings.StringSetting, *settings.StateMachineSetting, *settings.ByteSizeSetting:
				requiredType = parser.TypeString
			case *settings.BoolSetting:
				requiredType = parser.TypeBool
			case *settings.IntSetting:
				requiredType = parser.TypeInt
			case *settings.FloatSetting:
				requiredType = parser.TypeFloat
			case *settings.EnumSetting:
				requiredType = parser.TypeAny
			case *settings.DurationSetting:
				requiredType = parser.TypeInterval
			default:
				return nil, errors.Errorf("unsupported setting type %T", setting)
			}

			var dummyHelper parser.IndexedVarHelper
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

func (n *setClusterSettingNode) Start(params runParams) error {
	ie := InternalExecutor{LeaseManager: params.p.LeaseMgr()}

	var reportedValue string
	if n.value == nil {
		if _, err := ie.ExecuteStatementInTransaction(
			params.ctx, "reset-setting", params.p.txn,
			"DELETE FROM system.settings WHERE name = $1", n.name,
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
			`UPSERT INTO system.settings (name, value, "lastUpdated", "valueType") VALUES ($1, $2, NOW(), $3)`,
			n.name, encoded, n.setting.Typ(),
		); err != nil {
			return err
		}
		reportedValue = parser.AsStringWithFlags(n.value, parser.FmtBareStrings)
	}

	return MakeEventLogger(params.p.LeaseMgr()).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogSetClusterSetting,
		0, /* no target */
		int32(params.p.evalCtx.NodeID),
		struct {
			SettingName string
			Value       string
			User        string
		}{n.name, reportedValue, params.p.session.User},
	)
}

func (n *setClusterSettingNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *setClusterSettingNode) Values() parser.Datums          { return nil }
func (n *setClusterSettingNode) Close(_ context.Context)        {}

func (p *planner) toSettingString(
	ctx context.Context,
	ie InternalExecutor,
	st *cluster.Settings,
	name string,
	setting settings.Setting,
	val parser.TypedExpr,
) (string, error) {
	d, err := val.Eval(&p.evalCtx)
	if err != nil {
		return "", err
	}

	switch setting := setting.(type) {
	case *settings.StringSetting:
		if s, ok := d.(*parser.DString); ok {
			if err := setting.Validate(string(*s)); err != nil {
				return "", err
			}
			return string(*s), nil
		}
		return "", errors.Errorf("cannot use %s %T value for string setting", d.ResolvedType(), d)
	case *settings.StateMachineSetting:
		if s, ok := d.(*parser.DString); ok {
			datums, err := ie.QueryRowInTransaction(
				ctx, "retrieve-prev-setting", p.txn, "SELECT value FROM system.settings WHERE name = $1", name,
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

			dStr, ok := datums[0].(*parser.DString)
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
		if b, ok := d.(*parser.DBool); ok {
			return settings.EncodeBool(bool(*b)), nil
		}
		return "", errors.Errorf("cannot use %s %T value for bool setting", d.ResolvedType(), d)
	case *settings.IntSetting:
		if i, ok := d.(*parser.DInt); ok {
			if err := setting.Validate(int64(*i)); err != nil {
				return "", err
			}
			return settings.EncodeInt(int64(*i)), nil
		}
		return "", errors.Errorf("cannot use %s %T value for int setting", d.ResolvedType(), d)
	case *settings.FloatSetting:
		if f, ok := d.(*parser.DFloat); ok {
			if err := setting.Validate(float64(*f)); err != nil {
				return "", err
			}
			return settings.EncodeFloat(float64(*f)), nil
		}
		return "", errors.Errorf("cannot use %s %T value for float setting", d.ResolvedType(), d)
	case *settings.EnumSetting:
		if i, intOK := d.(*parser.DInt); intOK {
			v, ok := setting.ParseEnum(settings.EncodeInt(int64(*i)))
			if ok {
				return settings.EncodeInt(v), nil
			}
			return "", errors.Errorf("invalid integer value '%d' for enum setting", *i)
		} else if s, ok := d.(*parser.DString); ok {
			str := string(*s)
			v, ok := setting.ParseEnum(str)
			if ok {
				return settings.EncodeInt(v), nil
			}
			return "", errors.Errorf("invalid string value '%s' for enum setting", str)
		}
		return "", errors.Errorf("cannot use %s %T value for enum setting, must be int or string", d.ResolvedType(), d)
	case *settings.ByteSizeSetting:
		if s, ok := d.(*parser.DString); ok {
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
		if f, ok := d.(*parser.DInterval); ok {
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

func datumAsString(session *Session, name string, value parser.TypedExpr) (string, error) {
	evalCtx := session.evalCtx()
	val, err := value.Eval(&evalCtx)
	if err != nil {
		return "", err
	}
	s, ok := parser.AsDString(val)
	if !ok {
		return "", fmt.Errorf("set %s: requires a string value: %s is a %s",
			name, value, val.ResolvedType())
	}
	return string(s), nil
}

func getStringVal(session *Session, name string, values []parser.TypedExpr) (string, error) {
	if len(values) != 1 {
		return "", fmt.Errorf("set %s: requires a single string value", name)
	}
	return datumAsString(session, name, values[0])
}

func (p *planner) SetDefaultIsolation(n *parser.SetDefaultIsolation) (planNode, error) {
	// Note: We also support SET DEFAULT_TRANSACTION_ISOLATION TO ' .... ' above.
	// Ensure both versions stay in sync.
	switch n.Isolation {
	case parser.SerializableIsolation:
		p.session.DefaultIsolationLevel = enginepb.SERIALIZABLE
	case parser.SnapshotIsolation:
		p.session.DefaultIsolationLevel = enginepb.SNAPSHOT
	default:
		return nil, fmt.Errorf("unsupported default isolation level: %s", n.Isolation)
	}
	return &zeroNode{}, nil
}

func setTimeZone(_ context.Context, session *Session, values []parser.TypedExpr) error {
	if len(values) != 1 {
		return errors.New("set time zone requires a single argument")
	}
	evalCtx := session.evalCtx()
	d, err := values[0].Eval(&evalCtx)
	if err != nil {
		return err
	}

	var loc *time.Location
	var offset int64
	switch v := parser.UnwrapDatum(d).(type) {
	case *parser.DString:
		location := string(*v)
		loc, err = timeutil.LoadLocation(location)
		if err != nil {
			var err1 error
			loc, err1 = timeutil.LoadLocation(strings.ToUpper(location))
			if err1 != nil {
				loc, err1 = timeutil.LoadLocation(strings.ToTitle(location))
				if err1 != nil {
					return fmt.Errorf("cannot find time zone %q: %v", location, err)
				}
			}
		}

	case *parser.DInterval:
		offset, _, _, err = v.Duration.Div(time.Second.Nanoseconds()).Encode()
		if err != nil {
			return err
		}

	case *parser.DInt:
		offset = int64(*v) * 60 * 60

	case *parser.DFloat:
		offset = int64(float64(*v) * 60.0 * 60.0)

	case *parser.DDecimal:
		sixty := apd.New(60, 0)
		ed := apd.MakeErrDecimal(parser.ExactCtx)
		ed.Mul(sixty, sixty, sixty)
		ed.Mul(sixty, sixty, &v.Decimal)
		offset = ed.Int64(sixty)
		if ed.Err() != nil {
			return fmt.Errorf("time zone value %s would overflow an int64", sixty)
		}

	default:
		return fmt.Errorf("bad time zone value: %s", d.String())
	}
	if loc == nil {
		loc = sqlbase.FixedOffsetTimeZoneToLocation(int(offset), d.String())
	}
	session.Location = loc
	return nil
}
