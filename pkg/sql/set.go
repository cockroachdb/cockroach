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
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Set sets session variables.
// Privileges: None.
//   Notes: postgres/mysql do not require privileges for session variables (some exceptions).
func (p *planner) Set(ctx context.Context, n *parser.Set) (planNode, error) {
	if n.Name == nil {
		// A client has sent the reserved internal syntax SET ROW ...
		// Reject it.
		return nil, errors.New("invalid statement: SET ROW")
	}

	name := parser.AsStringWithFlags(n.Name, parser.FmtBareIdentifiers)
	setMode := n.SetMode

	if setMode == parser.SetModeClusterSetting {
		return p.setClusterSetting(ctx, name, n.Values)
	}

	// By using VarName.String() here any variables that are keywords will
	// be double quoted.
	typedValues := make([]parser.TypedExpr, len(n.Values))
	for i, expr := range n.Values {
		typedValue, err := parser.TypeCheck(expr, nil, parser.TypeString)
		if err != nil {
			return nil, err
		}
		typedValues[i] = typedValue
	}

	v, ok := varGen[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("unknown variable: %q", name)
	}

	if len(n.Values) == 0 {
		setMode = parser.SetModeReset
	}

	switch setMode {
	case parser.SetModeAssign:
		if v.Set == nil {
			return nil, fmt.Errorf("variable \"%s\" cannot be changed", name)
		}
		if err := v.Set(ctx, p.session, typedValues); err != nil {
			return nil, err
		}
	case parser.SetModeReset:
		if v.Reset == nil {
			return nil, fmt.Errorf("variable \"%s\" cannot be reset", name)
		}
		if err := v.Reset(p.session); err != nil {
			return nil, err
		}
	}

	return &emptyNode{}, nil
}

func (p *planner) setClusterSetting(
	ctx context.Context, name string, v []parser.Expr,
) (planNode, error) {
	if err := p.RequireSuperUser("SET CLUSTER SETTING"); err != nil {
		return nil, err
	}
	r := p.session.execCfg.Settings.Registry
	typ, ok := r.Lookup(name)
	if !ok {
		return nil, errors.Errorf("unknown cluster setting '%s'", name)
	}

	name = strings.ToLower(name)
	ie := InternalExecutor{LeaseManager: p.LeaseMgr()}

	switch len(v) {
	case 0:
		if _, err := ie.ExecuteStatementInTransaction(
			ctx, "update-setting", p.txn, "DELETE FROM system.settings WHERE name = $1", name,
		); err != nil {
			return nil, err
		}
	case 1:
		// TODO(dt): validate and properly encode str according to type.
		encoded, err := p.toSettingString(ctx, ie, name, typ, v[0])
		if err != nil {
			return nil, err
		}
		upsertQ := `UPSERT INTO system.settings (name, value, "lastUpdated", "valueType") VALUES ($1, $2, NOW(), $3)`
		if _, err := ie.ExecuteStatementInTransaction(
			ctx, "update-setting", p.txn, upsertQ, name, encoded, typ.Typ(),
		); err != nil {
			return nil, err
		}
	default:
		return nil, errors.Errorf("SET %q requires a single value", name)
	}
	return &emptyNode{}, nil
}

func (p *planner) toSettingString(
	ctx context.Context, ie InternalExecutor, name string, setting settings.Setting, raw parser.Expr,
) (string, error) {
	typeCheckAndParse := func(t parser.Type, f func(parser.Datum) (string, error)) (string, error) {
		typed, err := parser.TypeCheckAndRequire(raw, nil, t, name)
		if err != nil {
			return "", err
		}
		d, err := typed.Eval(&p.evalCtx)
		if err != nil {
			return "", err
		}
		return f(d)
	}

	switch setting := setting.(type) {
	case *settings.StringSetting:
		return typeCheckAndParse(parser.TypeString, func(d parser.Datum) (string, error) {
			if s, ok := d.(*parser.DString); ok {
				if err := setting.Validate(string(*s)); err != nil {
					return "", err
				}
				return string(*s), nil
			}
			return "", errors.Errorf("cannot use %s %T value for string setting", d.ResolvedType(), d)
		})
	case *settings.StateMachineSetting:
		return typeCheckAndParse(parser.TypeString, func(d parser.Datum) (string, error) {
			if s, ok := d.(*parser.DString); ok {
				datums, err := ie.QueryRowInTransaction(
					ctx, "retrieve-prev-setting", p.txn, "SELECT value FROM system.settings WHERE name = $1", name,
				)
				if err != nil {
					return "", err
				}
				var prevRawVal []byte
				if len(datums) != 0 {
					dStr, ok := datums[0].(*parser.DString)
					if !ok {
						return "", errors.New("the existing value is not a string")
					}
					prevRawVal = []byte(string(*dStr))
				}
				newBytes, _, err := setting.Validate(prevRawVal, (*string)(s))
				if err != nil {
					return "", err
				}
				return string(newBytes), nil
			}
			return "", errors.Errorf("cannot use %s %T value for string setting", d.ResolvedType(), d)
		})
	case *settings.BoolSetting:
		return typeCheckAndParse(parser.TypeBool, func(d parser.Datum) (string, error) {
			if b, ok := d.(*parser.DBool); ok {
				return settings.EncodeBool(bool(*b)), nil
			}
			return "", errors.Errorf("cannot use %s %T value for bool setting", d.ResolvedType(), d)
		})
	case *settings.IntSetting:
		return typeCheckAndParse(parser.TypeInt, func(d parser.Datum) (string, error) {
			if i, ok := d.(*parser.DInt); ok {
				if err := setting.Validate(int64(*i)); err != nil {
					return "", err
				}
				return settings.EncodeInt(int64(*i)), nil
			}
			return "", errors.Errorf("cannot use %s %T value for int setting", d.ResolvedType(), d)
		})
	case *settings.FloatSetting:
		return typeCheckAndParse(parser.TypeFloat, func(d parser.Datum) (string, error) {
			if f, ok := d.(*parser.DFloat); ok {
				if err := setting.Validate(float64(*f)); err != nil {
					return "", err
				}
				return settings.EncodeFloat(float64(*f)), nil
			}
			return "", errors.Errorf("cannot use %s %T value for float setting", d.ResolvedType(), d)
		})
	case *settings.EnumSetting:
		return typeCheckAndParse(parser.TypeAny, func(d parser.Datum) (string, error) {
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
		})
	case *settings.ByteSizeSetting:
		return typeCheckAndParse(parser.TypeString, func(d parser.Datum) (string, error) {
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
		})
	case *settings.DurationSetting:
		return typeCheckAndParse(parser.TypeInterval, func(d parser.Datum) (string, error) {
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
		})
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
	return &emptyNode{}, nil
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
