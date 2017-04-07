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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
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

	name := n.Name.String()
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
		if err := v.Set(ctx, p, typedValues); err != nil {
			return nil, err
		}
	case parser.SetModeReset:
		if v.Reset == nil {
			return nil, fmt.Errorf("variable \"%s\" cannot be reset", name)
		}
		if err := v.Reset(p); err != nil {
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
		encoded, err := p.toSettingString(v[0])
		if err != nil {
			return nil, err
		}
		upsertQ := "UPSERT INTO system.settings (name, value, lastUpdated, valueType) VALUES ($1, $2, NOW(), $3)"
		if _, err := ie.ExecuteStatementInTransaction(
			ctx, "update-setting", p.txn, upsertQ, name, encoded, "s",
		); err != nil {
			return nil, err
		}
	default:
		return nil, errors.Errorf("SET %q requires a single value", name)
	}
	return &emptyNode{}, nil
}

func (p *planner) toSettingString(raw parser.Expr) (string, error) {
	// TODO(dt): typecheck and handle according to setting's desired type.
	typed, err := parser.TypeCheckAndRequire(raw, nil, parser.TypeString, "SET")
	if err != nil {
		return "", err
	}
	d, err := typed.Eval(&p.evalCtx)
	if err != nil {
		return "", err
	}
	return string(parser.MustBeDString(d)), nil
}

func (p *planner) getStringVal(name string, values []parser.TypedExpr) (string, error) {
	if len(values) != 1 {
		return "", fmt.Errorf("set %s: requires a single string value", name)
	}
	val, err := values[0].Eval(&p.evalCtx)
	if err != nil {
		return "", err
	}
	s, ok := parser.AsDString(val)
	if !ok {
		return "", fmt.Errorf("set %s: requires a single string value: %s is a %s",
			name, values[0], val.ResolvedType())
	}
	return string(s), nil
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

func (p *planner) SetTimeZone(n *parser.SetTimeZone) (planNode, error) {
	typedValue, err := parser.TypeCheck(n.Value, nil, parser.TypeInt)
	if err != nil {
		return nil, err
	}
	d, err := typedValue.Eval(&p.evalCtx)
	if err != nil {
		return nil, err
	}

	var loc *time.Location
	var offset int64
	switch v := parser.UnwrapDatum(d).(type) {
	case *parser.DString:
		location := string(*v)
		loc, err = timeutil.LoadLocation(location)
		if err != nil {
			return nil, fmt.Errorf("cannot find time zone %q: %v", location, err)
		}

	case *parser.DInterval:
		offset, _, _, err = v.Duration.Div(time.Second.Nanoseconds()).Encode()
		if err != nil {
			return nil, err
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
			return nil, fmt.Errorf("time zone value %s would overflow an int64", sixty)
		}

	default:
		return nil, fmt.Errorf("bad time zone value: %v", n.Value)
	}
	if loc == nil {
		loc = time.FixedZone(d.String(), int(offset))
	}
	p.session.Location = loc
	return &emptyNode{}, nil
}
